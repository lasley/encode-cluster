#!/usr/bin/env python
import json
from threading import Thread
from batch_transcode.transcode import *
from custom_socket import socket_functions
from settings import IN_DIR, OUT_DIR, PORT, HOST, PROXY_HOST, PROXY_PORT

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)

class encode_cluster_server(transcode):
    SETTINGS_FILE =  os.path.join(os.path.dirname(__file__),
                                  'batch_transcode', 'video_settings.xml')
    def __init__(self, outdir, indir):
        super(encode_cluster_server, self).__init__(outdir)
        self.client_threads = []
        self.thread_map, self.client_files = {}, {}
        SOCKET_ARGS = {
            'transcode_done':self.transcode_done
        }
        self.server = socket_functions.custom_server(
            HOST, PORT, SOCKET_ARGS, ['transcode_done'])
        self.server.start()
        self.encode_directory(indir)
        self.new_files = []
    
    def release_client(self, cluster_id):
        self.client_threads[cluster_id] = False

    def transcode_done(self, output):
        logging.debug('Transcode_Done %s'%str(output))
        
        client_files = self.client_files[output['cluster_id']]
        client_files['demuxed'][client_files['waiting_transcode']] = output['new_file']
        client_files['cleanup_files'].append(client_files['demuxed'][client_files['waiting_transcode']])
        logging.debug('Transcoded %s' % self.client_files[output['cluster_id']]['demuxed'][client_files['waiting_transcode']])
        
        duped = self.compare_tracks(self.client_files[output['cluster_id']]['demuxed'])
        track_order = self.choose_track_order(client_files['media_info'])
        logging.debug('Track Order Chosen!')
        
        try:        
            new_file = transcode.remux(self.client_files[output['cluster_id']]['demuxed']
                                       ,client_files['media_info'],
                                       client_files['new_file'],duped,track_order,
                                       dry_run=self.DRY_RUNS['remux'])
            self.new_files.append(new_file)
            logging.debug('New File Success %s' % client_files['new_file'])
            
        except Exception as e:
            logging.error(repr(e))
            #   Implement #20
            fh = open(self.log_file, 'a')
            fh.write( repr(e) + ' - ' + repr(output) + ' - ' + repr(client_files) + "\n\n" ) 
            fh.close()
            
        try:
            self.release_client(output['cluster_id'])
        except (IndexError, KeyError):
            logging.debug(repr(self.client_threads))
            logging.debug('Cluster %s not found in clusters %s' % (
                output['cluster_id'], repr(self.client_threads)))

    def client_transcode(self, old_file, new_file, transcode_settings,cluster_info):
        
        try:
            self._client_file_cleanup(cluster_info[1])
            self.client_files[cluster_info[1]] = { 'media_info': self.media_info(old_file),
                                                    'new_file' : new_file,
                                                    'cleanup_files':[old_file] }
            media_info = self.client_files[cluster_info[1]]['media_info']
            self.client_files[cluster_info[1]]['demuxed'] = self.demux(
                old_file, media_info, self.encode_dir, dry_run=self.DRY_RUNS['demux'])
            self.client_files[cluster_info[1]]['cleanup_files'].extend(
                self.client_files[cluster_info[1]]['demuxed'])
            #media_info['tracks'][i+1]   :   mux_files[i]
            
            if len(media_info['id_maps']['Video']) > 1:
                raise EnvironmentError('>1 Video Track Support Not Implemented') # <@todo #3
            for vid_id in media_info['id_maps']['Video']:
                self.client_files[cluster_info[1]]['waiting_transcode'] = vid_id-1 
                cmd = {
                    'cmd'   :   'transcode', 
                    'old_file':old_file,
                    'new_file':os.path.join(
                                        self.encode_dir,
                                        u'%s.%s'%(os.path.basename(old_file),
                                                  self.TRANSCODE_SETTINGS['container'])
                                    ),
                    'media_info':media_info['tracks'][vid_id],
                    'new_settings':transcode_settings,
                    'dry_run':self.DRY_RUNS['transcode'],
                    'cluster_id': cluster_info[1],
                }
                logging.debug('Sending %s' % repr(cmd))
                self.server.send_str(json.dumps(cmd),[cluster_info[0]])
                return True #< @todo #3 
        
        except Exception as e:
            logging.error(repr(e))
            #   Implement #20
            fh = open(self.log_file, 'a')
            fh.write( repr(e) + ' - ' + repr(old_file) + ' - ' + repr(self.client_files) + "\n\n" ) 
            fh.close()
            os.rename(old_file, os.path.join(self.error_dir, os.path.basename(old_file)))
            self.release_client(cluster_info[1])

    def _client_file_cleanup(self,cluster_id):
        leftovers = []
        try:
            for i in xrange(0, len(self.client_files[cluster_id]['cleanup_files'])):
                cleanup_file = self.client_files[cluster_id]['cleanup_files'].pop(0)
                try:
                    logging.debug( 'Removing %s' % cleanup_file)
                    os.remove(cleanup_file)
                except OSError:
                    logging.error('Could not delete %s' % cleanup_file)
                    leftovers.append(cleanup_file)
            self.client_files[cluster_id]['cleanup_files'] = leftovers
        except KeyError: #< No cleanup files
            pass
        return True
    
    def encode_directory(self, inpath=None):
        '''
            Encodes an entire directory...cluster style!
            self.SELF_THREADS for self too
            
            @param  String  inpath  Directory to encode
            
            @return List    Newly created files
        '''

        def thread(root, new_root, file_name, extension,
                   transcode_settings, clustered=False):
            old_file = os.path.join(root, '%s%s' % (file_name, extension))
            new_file = os.path.join(new_root, '%s%s' % (file_name, extension))
            thread = Thread(target=self.client_transcode,
                                      name=clustered[1],
                                      args=(old_file, new_file,
                                            transcode_settings, clustered))
            thread.daemon = True
            thread.start()
            return True
                    
        for root, dirs, files in os.walk(inpath):
            
            dirs.sort()
            files.sort()
            
            if '.Apple' not in root:
                new_root = os.path.join(self.finished_dir,root.replace(inpath,''))
                if not os.path.isdir(new_root):
                    os.mkdir(new_root)
                try:
                    with open(os.path.join(root, self.SETTINGS_FILE)) as f: pass
                    transcode_settings = self.parse_video_settings(
                        os.path.join(root, self.SETTINGS_FILE))
                    print transcode_settings
                except IOError:
                    transcode_settings = {}

                for file_name in files:
                    file_name,extension = os.path.splitext(file_name)
                    child_took_it = False
                    if extension in self.VID_EXTS:
                        while not child_took_it:
                            #   Implement Socket Encoding...
                            for client in self.server.clients.values():
                                try:
                                    if self.client_threads[self.thread_map[client]]:
                                        continue
                                except (KeyError, IndexError):
                                    if thread(root, new_root, file_name, extension,
                                              transcode_settings, [client,len(self.client_threads)]):
                                        self.thread_map[client] = len(self.client_threads)
                                        self.client_threads.append( True )
                                        child_took_it = True
                                        break
                                        
                                if thread(root, new_root, file_name, extension,
                                          transcode_settings, [client,self.thread_map[client]]):
                                    self.client_threads[self.thread_map[client]] = True
                                    child_took_it = True
                                    break

                            time.sleep(1)  #<   Wait a bit
    

class encode_cluster_client(transcode):
    
    def __init__(self, outdir):
        ##  Init
        ##  @param  Str proxy_host  SOCKS proxy host
        ##  @param  Int proxy_port  SOCKS proxy port
        super( encode_cluster_client, self).__init__(outdir)
        SOCKET_ARGS = {
            'encode_it':self.encode_it,
            'transcode':self.transcode,
        }
        self.socket = socket_functions.custom_client(
            HOST, PORT, PROXY_HOST, PROXY_PORT, SOCKET_ARGS,['encode_it'])
        self.socket.recv()
        
    def __del__(self):
        self.kill_server()
        self.recv_thread.stop()
        
    def kill_server(self):
        logging.debug('Exiting Server..')
        for client in self.clients.values(): #< Kill connections
            client.close()
        self.server.close()
        exit(0)
        
    def encode_it(self,options):
        output = {
            'cmd' : 'transcode_done',
            'new_file': super(encode_cluster_client, self).encode_it(
                os.path.join(IN_DIR,options['old_file']),
                os.path.join(self.finished_dir,options['new_file']),
                options['transcode_settings']),
            'cluster_id':options['cluster_id']
        }
        print 'Done'
        print output
        self.socket.send_str(json.dumps(output))
        
    def transcode(self,options):
        cluster_id = options['cluster_id']
        del options['cluster_id'], options['cmd']
        output = {
            'cmd' : 'transcode_done',
            'new_file': super(encode_cluster_client, self).transcode(**options),
            'cluster_id':cluster_id
        }
        logging.debug( output )
        self.socket.send_str(json.dumps(output))


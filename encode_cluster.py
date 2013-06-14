#!/usr/bin/env python
import json
from batch_transcode.transcode import *
from custom_socket import socket_functions
from settings import IN_DIR, OUT_DIR

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)

class encode_cluster_server(transcode):
    SELF_THREADS = 0
    def __init__(self, outdir, indir):
        super( encode_cluster_server, self).__init__(outdir)
        self.client_threads = []
        self.thread_map = {}
        SOCKET_ARGS = {
            'transcode_done':self.transcode_done
        }
        self.server = socket_functions.custom_server(SOCKET_ARGS,['transcode_done',])
        self.server.start()
        self.encode_directory(indir)
    
    def transcode_done(self, output):
        logging.debug(str(output))
        self.client_threads[output['client_id']] = False
        # @todo More of this
    
    def client_transcode(self, old_file, new_file, transcode_settings):
        cmd = {
            'cmd'   :   'encode_it', #< @todo use transcode; this creates a filesystem structure dependency
            'old_file': old_file.replace(IN_DIR,''),
            'new_file': new_file.replace(OUT_DIR,''),
            'transcode_settings':transcode_settings,
            'cluster_id': clustered[1],
        }
        logging.debug('Sending %s' % repr(cmd))
        self.server.send_str(json.dumps(cmd),[clustered[0]])
        return True
    
    def encode_directory(self,inpath=None):
        '''
            Encodes an entire directory...cluster style!
            self.SELF_THREADS for self too
            
            @param  String  inpath  Directory to encode
            
            @return List    Newly created files
        '''
        self.new_files = []
        self.worker_threads = {}
        def thread(root,new_root,file_name,extension,transcode_settings,clustered=False):
            old_file = os.path.join(root,'%s%s'%(file_name,extension))
            new_file = os.path.join(new_root, file_name, extension)
            if clustered:
                self.client_transcode(old_file, new_file, transcode_settings)
            else:
                self.new_files.append( self.encode_it(old_file, new_file,transcode_settings) )
                del self.worker_threads[file_name]
                os.unlink(os.path.join(root,file_name,extension))
                logging.debug('DELETED: %s'%os.path.join(root,file_name,extension))
        for root, dirs, files in os.walk(inpath):
            dirs.sort()
            files.sort()
            if '.Apple' not in root:
                new_root = os.path.join(self.finished_dir,root.replace(inpath,''))
                if not os.path.isdir(new_root):
                    os.mkdir(new_root)
                try:
                    with open(os.path.join(root,self.settings_file)) as f: pass
                    transcode_settings = self.parse_video_settings(os.path.join(root,self.settings_file))
                    print transcode_settings
                except IOError:
                    transcode_settings = {}

                for file_name in files:
                    file_name,extension = os.path.splitext(file_name)
                    child_took_it = False
                    if extension in self.vid_exts:
                        while len(self.worker_threads) >= self.SELF_THREADS:
                            
                            #   Implement Socket Encoding...
                            for client in self.server.clients.values():
                                try:
                                    if self.client_threads[self.thread_map[client]]:
                                        break
                                except (KeyError, IndexError):
                                    if thread(root,new_root,file_name,extension,transcode_settings,[client,len(self.client_threads)]):
                                        self.thread_map[client] = len(self.client_threads)
                                        self.client_threads.append( True )
                                        child_took_it = True
                                        break
                                        
                                if thread(root,new_root,file_name,extension,transcode_settings,[client,len(self.client_threads)]):
                                    self.client_threads[self.thread_map[client]] = True
                                    child_took_it = True

                            if child_took_it: #< @todo get rid of this...
                                break
                            
                            time.sleep(1)  #<   Wait a bit
                            
                        if child_took_it: #< @todo get rid of this...
                            continue

                        self.worker_threads[file_name] = (threading.Thread(target=thread,name=file_name,args=(root,new_root,file_name,extension,transcode_settings)))
                        self.worker_threads[file_name].daemon = True
                        self.worker_threads[file_name].start()

class encode_cluster_client(transcode):
    
    def __init__(self, outdir):
        ##  Init
        ##  @param  Str proxy_host  SOCKS proxy host
        ##  @param  Int proxy_port  SOCKS proxy port
        super( encode_cluster_client, self).__init__(outdir)
        SOCKET_ARGS = {
            'encode_it':self.encode_it
        }
        self.socket = socket_functions.custom_client(SOCKET_ARGS)
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
            'new_file': super(encode_cluster_client, self).encode_it(options['old_file'],options['new_file'],options['transcode_settings'])
        }
        self.socket.send_str(json.dumps(output))

#!/usr/bin/env python
import json
import logging
from batch_transcode import transcode
from custom_socket import socket_functions

logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)

class encode_cluster_server(transcode):
    SELF_THREADS = 1
    def __init__(self, indir, outdir):
        super( encode_cluster_server, self).__init__(indir, outdir)
        self.client_threads = []
        self.thread_map = {}
        SOCKET_ARGS = {



        }
        self.server = socket_functions.custom_server(SOCKET_ARGS,['get_busy',])
        self.server.run()
    
    def transcode_done(self, output):
        logging.debug(str(output))
        self.client_threads[output['client_id']] = False
        # @todo More of this
    
    def encode_directory(self,inpath):
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
                cmd = {
                    'cmd'   :   'encode_it', #< @todo use transcode; this creates a filesystem structure dependency
                    'old_file': old_file,
                    'new_file': new_file,
                    'transcode_settings':transcode_settings,
                    'cluster_id': clustered[1],
                }
                self.server.send_str(json.dumps(cmd),[clustered[0]])
                return True
            else:
                self.new_files.append( self.encode_it(old_file, new_file,transcode_settings) )
                del self.worker_threads[file_name]
                if True not in self.dry_runs:   #<  Delete the file if not testing or specified
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
                                        
                            if thread(root,new_root,file_name,extension,transcode_settings,client):
                                self.client_threads[self.thread_map[client]] = True
                                child_took_it = True

                            if child_took_it: #< @todo get rid of this...
                                break
                            
                            time.sleep(10)  #<   Wait a bit
                            
                        if child_took_it: #< @todo get rid of this...
                            break

                        self.worker_threads[file_name] = (threading.Thread(target=thread,name=file_name,args=(root,new_root,file_name,extension,transcode_settings)))
                        self.worker_threads[file_name].daemon = True
                        self.worker_threads[file_name].start()

class encode_cluster_client( transcode ):
    
    def __init__(self, proxy_host=None, proxy_port=8080):
        ##  Init
        ##  @param  Str proxy_host  SOCKS proxy hsot
        ##  @param  Int proxy_port  SOCKS proxy port
        SOCKET_ARGS = {
            
        }
        self.socket = socket_functions.custom_client(SOCKET_ARGS)
        self.recv_thread = worker_thread(self.socket.recv)
        self.recv_thread.finished.connect(self.thread_finish)
        self.recv_thread.start()
        
    def encode_it(self,options):
        output = {
            'cmd' : 'transcode_done',
            'new_file': super(encode_cluster_client, self).encode_it(options['old_file'],options['new_file'],options['transcode_settings'])
        }
        self.socket.send_str(json.dumps(output))

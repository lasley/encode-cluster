#!/usr/bin/env python
import os
HOST = '192.168.69.100'
PORT = 8888
PROXY_HOST = None
PROXY_PORT = None
IN_DIR = os.path.join('Z:\\','3-transcode_bucket')
OUT_DIR = os.path.join('Z:\\','3.5-transcoding')   
if not os.path.isdir(IN_DIR):
    IN_DIR = os.path.join('/media','Motherload','3-transcode_bucket')
    OUT_DIR = os.path.join('/media','Motherload','3.5-transcoding')
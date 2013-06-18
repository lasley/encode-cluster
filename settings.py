#!/usr/bin/env python
import os
IN_DIR = os.path.join('Z:\\','3-transcode_bucket')
OUT_DIR = os.path.join('Z:\\','3.5-transcoding')   
if not os.path.isdir(IN_DIR):
    IN_DIR = os.path.join('/media','Motherload','3-transcode_bucket')
    OUT_DIR = os.path.join('/media','Motherload','3.5-transcoding')
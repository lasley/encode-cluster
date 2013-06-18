#!/usr/bin/env python
from encode_cluster import encode_cluster_server
from settings import IN_DIR, OUT_DIR

if __name__ == '__main__':
    server = encode_cluster_server(OUT_DIR,IN_DIR)
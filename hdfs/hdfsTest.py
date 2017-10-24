# -*- coding: utf-8 -*-
"""

@author: xiongz
"""
from hdfs import Client
# HDFSNN = "http://jp-bigdata-03:50070"
# client = Client(HDFSNN,root="/",timeout=100,session=False)
from hdfs import InsecureClient
# client = InsecureClient('http://jp-bigdata-03:50070', user='xiongz')
client = InsecureClient('http://jp-bigdata-03:50070', user='xiongz')

def ls(dir):
	return client.list(dir)

def mkdir(dir):
	client.makedirs(dir)

def rm(dir):
	client.delete(dir)

def rm_r(dir):
	client.delete(dir,recursive=True)

# 其他参数：upload(hdfs_path, local_path, overwrite=False, n_threads=1, temp_dir=None, 
#                                  chunk_size=65536,progress=None, cleanup=True, **kwargs)
#                overwrite：是否是覆盖性上传文件
#                n_threads：启动的线程数目
#                temp_dir：当overwrite=true时，远程文件一旦存在，则会在上传完之后进行交换
#                chunk_size：文件上传的大小区间
#                progress：回调函数来跟踪进度，为每一chunk_size字节。它将传递两个参数，文件上传的路径和传输的字节数。一旦完成，-1将作为第二个参数
#                cleanup：如果在上传任何文件时发生错误，则删除该文件
def upload(dirHadoop,dirLocal):
	client.upload(dirHadoop,dirLocal)

def download(dirHadoop,dirLocal):
	client.download(dirHadoop,dirLocal)

def read(dir):
	with client.read(dir) as file:
		a = file.read().split("/n")
		for i in a:
			print i

# def write(self, hdfs_path, data=None, overwrite=False, permission=None,
#     blocksize=None, replication=None, buffersize=None, append=False,
#     encoding=None)
"""Create a file on HDFS.
:param hdfs_path: Path where to create file. The necessary directories will
  be created appropriately.
:param data: Contents of file to write. Can be a string, a generator or a
  file object. The last two options will allow streaming upload (i.e.
  without having to load the entire contents into memory). If `None`, this
  method will return a file-like object and should be called using a `with`
  block (see below for examples).
:param overwrite: Overwrite any existing file or directory.
:param permission: Octal permission to set on the newly created file. Leading zeros may be omitted.
:param blocksize: Block size of the file.
:param replication: Number of replications of the file.
:param buffersize: Size of upload buffer.
:param append: Append to a file rather than create a new one.
:param encoding: Encoding used to serialize data written.
"""
def write():
	with client.write(hdfs_path="/user/xiongz/ltest1.py",encoding='utf-8',append=True) as writer:
		for i in range(12,19):
			writer.write(str(i)+"\n")


def create_file(dir="/user/xiongz/test.log"):
	client.write(hdfs_path=dir,data="",encoding='utf-8')

print ls('/')
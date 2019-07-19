#from here:
#https://medium.com/@niyazi_erd/aws-s3-multipart-upload-with-python-and-boto3-9d2a0ef9b085

#also here:
#https://stackoverflow.com/questions/52825430/stream-large-string-to-s3-using-boto3

import boto3
import os
import sys


sys.path.append("classes")

#from ExparteLog import ExparteLog
from GameBotoS3 import GameBotoS3


filename = 'GameUnityNotMac_20190624.zip'
filepath =  '/webroot_game/' + filename

bucket_name = "criminalprocedure"
s3 = GameBotoS3(bucket_name)
s3.multi_part_upload_with_s3(filepath, filename)
	
	
'''
# Multipart upload
config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10, multipart_chunksize=1024 * 25, use_threads=True)
file_path =  '/webroot_game/builds_20190624_renpy/PulledOver-4.6-pc.zip'
key_path = 'multipart_files/largefile.pdf'
s3.meta.client.upload_file(file_path, bucket_name, key_path,
						ExtraArgs={'ACL': 'public-read', 'ContentType': 'text/pdf'},
						Config=config,
						Callback=ProgressPercentage(file_path)
						)

						
class ProgressPercentage(object):
def __init__(self, filename):
	self._filename = filename
	self._size = float(os.path.getsize(filename))
	self._seen_so_far = 0
	self._lock = threading.Lock()
def __call__(self, bytes_amount):
	# To simplify we'll assume this is hooked up
	# to a single filename.
	with self._lock:
		self._seen_so_far += bytes_amount
		percentage = (self._seen_so_far / self._size) * 100
		sys.stdout.write(
			"\r%s  %s / %s  (%.2f%%)" % (
				self._filename, self._seen_so_far, self._size,
				percentage))
		sys.stdout.flush()


if __name__ == '__main__': 

multi_part_upload_with_s3()
'''
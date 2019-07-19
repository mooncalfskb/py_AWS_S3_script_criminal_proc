import os
import sys
import csv
#pip3.6 install boto3
import boto3
from boto3.s3.transfer import TransferConfig
import threading

class GameBotoS3:

	def __init__(self, bucket_name):
	
		#omg. this was so much easier than https api. thank you boto
		#https://blog.ipswitch.com/managing-amazon-s3-with-python
		#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html

		#get api key
		with open('/webroot_auth/aws_s3_key_fcc_json.txt', 'rt') as csvfile:
			mysql_auth = csv.reader(csvfile, delimiter=',', quotechar='|')
			data = list(mysql_auth)
			AWSAccessKeyId = data[2][0]
			AWSSecretKey = data[2][1]

		#instantiate bucket
		self.bucket_name = bucket_name
		self.s3 = boto3.resource('s3', region_name='us-west-2', aws_access_key_id=AWSAccessKeyId, aws_secret_access_key=AWSSecretKey)
		print(self.s3)
		self.bucket = self.s3.Bucket(self.bucket_name)
		self.bytes_amount = 1024
	

	def getCurrentDir(self):
		return os.getcwd()
		
		
	def multi_part_upload_with_s3(self, filepath, filename):			
		
		# Multipart upload
		config = TransferConfig(multipart_threshold=self.bytes_amount * 25, max_concurrency=10, multipart_chunksize=self.bytes_amount * 25, use_threads=True)
		key_path = '20190624_backup/' + filename
		
		size = float(os.path.getsize(filepath))
		self.s3.meta.client.upload_file(filepath, self.bucket_name, key_path,
								ExtraArgs={'ACL': 'private', 'ContentType': 'application/zip'},
								Config=config,
								Callback=ProgressPercentage(filepath)
								)
		#Callback=self.get_progress_percentage(filepath, filename, size)
		
	#note: I tried to move this to a function within this class but it did not work.
	#ended up putting the author's class at the end of this file.
	def get_progress_percentage(self, filepath, filename, size):
		seen_so_far = 0
		
		with threading.Lock():
			while seen_so_far < size:
				seen_so_far += self.bytes_amount
				percentage = (seen_so_far / size) * 100
				sys.stdout.write(
					"\r%s  %s / %s  (%.2f%%)" % (
						filename, seen_so_far, size,
						percentage))
				sys.stdout.flush()
		
		
		
		
	##############################################
	# everything below here from exparte	
		
	def loopThroughFolderAndMove(self, filepath, foldername):
		with open(filepath) as f:
			src_files = f.readlines()
		# you may also want to remove whitespace characters like `\n` at the end of each line
		src_files = [x.strip() for x in src_files] 

		for src_file in src_files:
			chars = len(foldername)
			dest_file = src_file[chars:]
			print("About To Move " + src_file)
			print("Moving to " + dest_file)
			self.copyFileAndDeleteOriginal(src_file, dest_file)			
			
	def loopThroughFolderAndFixNames(self, filepath, foldername):
		with open(filepath) as f:
			src_files = f.readlines()
		# you may also want to remove whitespace characters like `\n` at the end of each line
		src_files = [x.strip() for x in src_files] 

		for src_file in src_files:
			if src_file[-4:] == ".txt":
				print("About To Move " + src_file)
				if src_file[0:3] == "fcc":
					name_list = src_file.split("_")
					fcc = name_list[0]
					unix_epoch = name_list[1].zfill(16)
					partition_list = name_list[2].split(".")
					parition = partition_list[0]
					dest_file = fcc + "_" + unix_epoch + "_" + parition + ".json"
					print(dest_file)
					print("About To Copy To  " + dest_file)
					self.copyFileAndDeleteOriginal(src_file, dest_file)
			else:
				continue			
		

	#function to write out a list of files in a S3 Folder.
	#writes to qa_files, which is in the git ignore.
	def writeOutFolderContents(self, folder_name, filepath):

		#you could also say "maxkeys, but I chose not to"
		test_keys = self.bucket.objects.filter(
			Delimiter='',
			EncodingType='url',
			Marker='',
			Prefix=folder_name,
		)

		contents_str = ""
		for test_key in test_keys:
			if test_key.key == folder_name:
				continue
			else:
				contents_str = contents_str + test_key.key + "\n"

		f = open(filepath, 'w')
		f.write(contents_str)  # python will convert \n to os.linesep
		f.close()
		

	def searchForOneFile(self, filename):

		#you could also say "maxkeys, but I chose not to"
		test_keys = self.bucket.objects.filter(
			Delimiter='',
			EncodingType='url',
			Marker='',
			Prefix=filename,
		)

		contents_str = ""
		for test_key in test_keys:
			if test_key.key == filename:
				print("Found " + filename + " on " + self.bucket_name)
				return True
		
		return False



	def copyFileAndDeleteOriginal(self, src_name, dest_name):
		self.bucket.copy({"Bucket": self.bucket_name, "Key": src_name}, dest_name)
		#delete examples
		#FCCExparteDataAll_001501-1513/fcc_0001501182600000_004000.json"
		#https://stackoverflow.com/questions/3140779/how-to-delete-files-from-amazon-s3-bucket
		self.s3.Object(self.bucket_name, src_name).delete()



class ProgressPercentage(object):

	def __init__(self, filename):
		self._filename = filename
		self._size = float(os.path.getsize(filename))
		self._seen_so_far = 0
		self._lock = threading.Lock()
		self._bytes_amount = 1024

	def __call__(self, bytes_amount):
		# To simplify we'll assume this is hooked up
		# to a single filename.
		with self._lock:
			self._seen_so_far += self._bytes_amount
			percentage = (self._seen_so_far / self._size) * 100
			sys.stdout.write(
				"\r%s  %s / %s  (%.2f%%)" % (
					self._filename, self._seen_so_far, self._size,
					percentage))
			sys.stdout.flush()		

#import mysql.connector
import sys
import datetime
import smtplib
import csv
from email.mime.text import MIMEText

class ExparteLog:

	def __init__(self, log_file, auth_csv_file, to_address):
		self.log = log_file
		self.to_address = to_address
		#get email from auth file
		with open(auth_csv_file, 'rt') as csvfile:
			email_auth = csv.reader(csvfile, delimiter=',', quotechar='|')
			data = list(email_auth)
			self.email_from_address = data[0][0]
			#print(self.email_from_address)
			self.email_from_password = data[0][1]
			#print(self.email_from_password)

	#write errors to log and email if description start with ERROR:
	def writeLog(self, description, error):
		try:
			ts = str(datetime.datetime.now())
			f = open(self.log,'a')
			f.write(ts + ":" + description + error + '\n\n') # python will convert \n to os.linesep
			f.close() # you can omit in most cases as the destructor will call it
		except: # catch *all* exceptions
			e = sys.exc_info()[0]
			print(e)
		try:
			#try emailing
			#only email if ERROR: is the first word of the description string
			#non errors are simply written to the log for reference...
			if len(description) > 5:
				error_check = description[0:5]
				if error_check == "ERROR":
					self.emailError(description, error, ts)
		except: # catch *all* exceptions
			e = sys.exc_info()[0]
			print(e)

	def emailError(self, description, error, ts):
		try:
			msg = MIMEText(error + "\n\n" + ts)
			# me == the sender's email address
			# you == the recipient's email address
			msg['Subject'] = description
			msg['From'] = self.email_from_address
			msg['To'] = self.to_address
			#note: in order to use gmail, I had to do set lesssecureapps to on:
			#https://myaccount.google.com/lesssecureapps
			s = smtplib.SMTP('smtp.gmail.com', 587)
			s.starttls()
			s.login(self.email_from_address, self.email_from_password)
			s.sendmail(self.email_from_address, self.to_address, msg.as_string())
			s.quit()
		except: # catch *all* exceptions
			e = sys.exc_info()[0]
			print(e)

		
		
#https://myaccount.google.com/lesssecureapps
# server = smtplib.SMTP('smtp.gmail.com', 587)
# server.starttls()
# server.login("YOUR EMAIL ADDRESS", "YOUR PASSWORD")
		
# python -m smtpd -n -c DebuggingServer localhost:1025
# python3 -m smtpd -n -c DebuggingServer localhost:1025

# Make sure to modify the mail-sending code to use the non-standard port number:
# 
# server = smtplib.SMTP(SERVER, 1025)
# server.sendmail(FROM, TO, message)
# server.quit()
import threading
import datetime
import time
import sys
import argparse
from threading import Thread
from multiprocessing.synchronize import BoundedSemaphore
from boto.s3.connection import S3Connection, OrdinaryCallingFormat

AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""

def s3connect(preserve_acl=False, insecure=False, ordinarycallingformat=False, host=None, debug=None):
	connection_parameters = {}
	connection_parameters['aws_access_key_id'] = AWS_ACCESS_KEY_ID
	connection_parameters['aws_secret_access_key'] = AWS_SECRET_ACCESS_KEY
	if ordinarycallingformat:
		connection_parameters['calling_format'] = OrdinaryCallingFormat()
	if insecure:
		connection_parameters['is_secure'] = False
	if host:
		connection_parameters['host'] = host
	c = S3Connection(**connection_parameters)
	if debug:
		c.debug = debug
	return c
	

def copy_s3_bucket(SOURCE_BUCKET, DEST_BUCKET, prefix=None, threads=10, preserve_acl=False, insecure=False, ordinarycallingformat=False, host=None, debug=None):
	"""
	Example usage: copy_s3_bucket(SOURCE_BUCKET='my-source-bucket', DEST_BUCKET='my-destination-bucket', prefix='parent/child/dir/', threads=20)
	"""
	# Init s3
	conn = s3connect(preserve_acl,insecure,ordinarycallingformat,host,debug)
	bucket = conn.get_bucket(SOURCE_BUCKET)
	dest_bucket = conn.get_bucket(DEST_BUCKET)

	# Filter by prefix
	rs = bucket.list()
	if prefix: rs = bucket.list(prefix)

	class CopyKey(Thread):
		def __init__ (self, key_name):
			Thread.__init__(self)
			self.key_name = key_name
			self.status = False
		def run(self):
			# We must create new bucket instances for each thread, passing the key is not threadsafe
			thread_conn = conn = s3connect(preserve_acl,insecure,ordinarycallingformat,host,debug)
			thread_bucket = conn.get_bucket(SOURCE_BUCKET)
			thread_dest_bucket = conn.get_bucket(DEST_BUCKET)
			thread_key = thread_bucket.get_key(self.key_name)

			# Only copy if not exists on dest bucket
			if not thread_dest_bucket.get_key(self.key_name):
				pool_sema.acquire()
				self.status = "%s : Sempahore Acquired, Copy Next" % datetime.datetime.now()
				try:
					thread_key.copy(DEST_BUCKET, self.key_name)
					self.status = "%s : Copy Success : %s" % (datetime.datetime.now(), self.key_name)
				except:
					self.status = "%s : Copy Error : %s" % (datetime.datetime.now(), sys.exc_info())
				finally:
					pool_sema.release()
			else:
				self.status = "%s : Key Already Exists, will not overwrite." % datetime.datetime.now()

	key_copy_thread_list = []
	pool_sema = BoundedSemaphore(value=threads)
	total_keys = 0

	# Request threads
	for key in rs:
		total_keys += 1
		print "%s : Requesting copy thread for key %s" % (datetime.datetime.now(), key.name)
		current = CopyKey(key.name)
		key_copy_thread_list.append(current)
		current.start()

		# Pause if max threads reached - note that enumerate returns all threads, including this parent thread
		if len(threading.enumerate()) >= threads:
			print "%s : Max Threads (%s) Reached: Pausing until threadcount reduces." % (datetime.datetime.now(), threads)
			while 1:
				if len(threading.enumerate()) < threads:
					print "%s : Continuing thread creation." % datetime.datetime.now()
					break
				time.sleep(1)

	for key_copy_thread in key_copy_thread_list:
		key_copy_thread.join(30) # Bring this particular thread to this current "parent" thread, blocks parent until joined or 30s timeout
		if key_copy_thread.isAlive():
			print "%s : TIMEOUT on key %s" % (datetime.datetime.now(), key_copy_thread.key_name)
			continue
		print "%s : Status Output: %s" % (datetime.datetime.now(), key_copy_thread.status)

	print "%s : Complete : %s Total Keys Requested" % (datetime.datetime.now(), total_keys)

def main():
	parser = argparse.ArgumentParser(
		formatter_class=argparse.RawDescriptionHelpFormatter,
		usage='%(prog)s [OPTIONS] SOURCE DESTINATION',
		add_help=False
		)
	parser.add_argument(
		'-a', '--access-key', metavar='KEY', dest='access_key',
		help='Your Access Key ID'
		)
	parser.add_argument(
		'-s', '--secret-key', metavar='SECRET', dest='secret_access_key',
		help='Your Secret Key.'
		)
	parser.add_argument(
		'--insecure', action='store_true',
		help='Connect without SSL'
		)
	parser.add_argument(
		'--ordinarycallingformat' , action='store_true',
		help='Connect with the old connection type ' + \
		'(no subdomain and Bucket upper case).'
		)
	parser.add_argument(
		'--host', metavar='HOST', default='s3.amazonaws.com',
		help='Specify a specific S3 endpoint to connect to via boto\'s ' + \
		'"host" connection argument (S3 only).'
		)
	parser.add_argument(
		'--threads', default='10', type=int,
		help='number of threads'
		)
	parser.add_argument(
		'-p', '--preserve-acl', dest='preserve_acl', action='store_true',
		help='Copy the ACL from the source key to the destination key'
		)
	parser.add_argument(
		'-d', '--debug', metavar='LEVEL', choices=[0, 1, 2], default=0,
		type=int,
		help='Level 0 means no debug output (default), 1 means normal ' + \
			'debug output from boto, and 2 means boto debug output plus ' + \
			'request/response output from httplib.'
		)
		

	parser.add_argument('SOURCE', help=argparse.SUPPRESS)
	parser.add_argument('DESTINATION', help=argparse.SUPPRESS)

	try:
		args = parser.parse_args()
	except argparse.ArgumentTypeError:
		pass

	global AWS_ACCESS_KEY_ID
	global AWS_SECRET_ACCESS_KEY
	AWS_ACCESS_KEY_ID = args.access_key
	AWS_SECRET_ACCESS_KEY = args.secret_access_key

	copy_s3_bucket(args.SOURCE, args.DESTINATION, threads=args.threads, insecure=args.insecure, ordinarycallingformat=args.ordinarycallingformat, preserve_acl=args.preserve_acl, host=args.host, debug=args.debug)

if __name__ == "__main__":
    main()

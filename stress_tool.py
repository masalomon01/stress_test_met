import numpy as np
import matplotlib.pyplot as pyplot
import time
import os
import sys
import threading
import collections
import csv
import time
import pycurl
from StringIO import StringIO
import gc
import Queue

graph = False
debug = False
result = Queue.Queue()
batch = Queue.Queue()
error_log = Queue.Queue()

current_dir = 'stress_result'+time.strftime("_%Y_%m_%d_%H_%M_%S")

def read_input(file_name='stress_input.csv'):
	print 'current input_filename {}'.format(file_name)
	with open(file_name,'r') as csvfile:
		reader = csv.DictReader(csvfile,dialect=csv.excel_tab,delimiter=',')
		input_data = []
		for row in reader:
			if int(row['enable']) == 1: # enable test case is 1, disable test case otherwise
				raw = (('group_name', row['group_name'] if row['group_name'] else None),
				('test_name', row['test_name'] if row['test_name'] else None),
				('server_name', row['server_name'] if row['server_name'] else None),
				('city', row['city'] if row['city'] else None),
				('trip_time', int(row['trip_time']) if row['trip_time'] else None),
				('sample_size' , int(row['sample_size']) if row['sample_size'] else None),
				('avg_users' , int(row['avg_users']) if row['avg_users'] else None),
				('mode', int(row['mode']) if row['mode'] else None),
				('username', row['username'] if row['username'] else None),
				('password', row['password'] if row['password'] else None),
				('url',row['url']))
				in_order = collections.OrderedDict(raw)
				input_data.append(in_order)

	return input_data


def summary_report(summary,file_name='stress_output_sum.csv'):
	with open(os.path.join(current_dir,file_name), 'w') as csvfile:
		fieldnames = list(summary[0][0].keys()) #'test_name','trip_time','latency_avg','error_rate'
		writer = csv.DictWriter(csvfile,dialect=csv.excel_tab,fieldnames=fieldnames,delimiter=',')
		for group_sum in summary:# diff group
			writer.writeheader() # write the header
			for group in group_sum: # same group
				dic_row = collections.OrderedDict()
				for k,v in group.iteritems():
					dic_row[k]= v
				writer.writerow(dic_row)
		
def write_output(group_output,file_name='stress_output.csv'): # per group
	with open(os.path.join(current_dir,file_name), 'w') as csvfile:
		fieldnames = list(group_output[0].keys())
		writer = csv.DictWriter(csvfile,dialect=csv.excel_tab,fieldnames=fieldnames,delimiter=',')
		writer.writeheader() # write the header
		sum_return = []
		sum_need = ['test_name','trip_time','latency_avg','error_rate']
		for test_dic in group_output:
			dic_row = collections.OrderedDict()
			sum_dic_row = collections.OrderedDict() # used for group sum for sum report
			for k,v in test_dic.iteritems():
				dic_row[k]= v
				if k in sum_need:
					sum_dic_row[k]=v
			sum_return.append(sum_dic_row) 
			writer.writerow(dic_row)
		return sum_return

def plot_check(sample, sample_size):
	if graph:
		count, bins, ignored = pyplot.hist(sample, sample_size) # s = array, bins ,normed=True
		pyplot.show()

def unique_count(sample,index = 0):
	his = {}
	for data in sample:
		if data[index] in his:
			his[data[index]] +=1
		else:
			his[data[index]] = 1
	return his

def result_count(sample):
	his = {}
	count = {}
	for data in sample:
		if data[0] in his:
			his[data[0]] += data[2]
			count[data[0]] += 1
		else:
			his[data[0]] = data[2]
			count[data[0]] = 1
	for key, sum_value in his.iteritems():
		his[key] = round(sum_value/float(count[key]),3)
	return his

def possion_pmf(avg_users):
	# Percent point function 
	x = np.arange(poisson.ppf(0.000001,avg_users),poisson.ppf(0.999999,avg_users))
	pyplot.figure(3)
	pyplot.plot(x, poisson.pmf(x, avg_users), 'bo', ms=8, label='poisson pmf')
	pyplot.show()
	
def poisson_dist(group_name,test_name,mode_name,avg_users,sample_size):
	sample = np.random.poisson(avg_users,sample_size)
	pyplot.figure(0,figsize=(10, 10))
	ax = pyplot.subplot(211) # numrows, numcols, fignum
	ax.set_ylabel('num of requests')
	ax.set_xlabel('num of time in seconds')
	ax.set_title('poisson_dist avg_users: {} for {} seconds'.format(avg_users,sample_size)) 
	pyplot.plot(sample,linewidth=2.0)
	return sample

def normal_dist(avg_users,std,sample_size):
	# mu,sigma = mean and standard deviation
	sample = np.random.normal(avg_users, std, sample_size)
	plot_check(sample, sample_size)
	return od

def multi_thread(sample_size):
	his = {}
	his[0] = sample_size # sends sample_size at once
	return his

def single_thread(sample_size):
	his = {}
	for i in range(sample_size): # 1 second per request
		his[i] = 1
	return his


def validation(urls,username=None,password=None,request_timeout=60):
	try:
		buffer = StringIO()
		c = pycurl.Curl()
		c.setopt(c.URL, urls)
		c.setopt(pycurl.CONNECTTIMEOUT, request_timeout)
		if username and password:
			c.setopt(pycurl.USERPWD, '%s:%s' % (username, password))
		c.setopt(c.WRITEDATA, buffer)
		c.perform()
		if c.getinfo(pycurl.HTTP_CODE) != 200:
			print "validation error {} exit".format(error)
			sys.exit()
		c.close()
	except Exception,error:
		print "validation error {}".format(error)
		sys.exit()
	return True

def all_validation(data,request_timeout):
	for i in data:
		open_url = i['url']
		username = i['username']
		password = i['password']

		if debug: print 'validating for {}'.format(i['test_name'])
		if username and password:
			valid = validation(open_url,username,password,request_timeout)
		else:	
			valid = validation(open_url,request_timeout)
		i['check'] = 1 if valid else 0
	return data
			

def query(delay_key,ThreadID,open_url,username=None,password=None,request_timeout=60): 
	try:
		# thread local variables
		start = threading.local()
		c = threading.local()
		end  = threading.local()
		diff = threading.local()
		buffer = threading.local()

		start = time.time()
		buffer = StringIO()
		c = pycurl.Curl()
		c.setopt(c.URL, open_url)
		c.setopt(pycurl.CONNECTTIMEOUT, request_timeout)
		c.setopt(pycurl.TIMEOUT, request_timeout)
		c.setopt(c.FAILONERROR, True)
		if username and password:
			c.setopt(pycurl.USERPWD, '%s:%s' % (username, password))
		c.setopt(c.WRITEDATA, buffer)
		c.perform()
		end = time.time()
		diff = round(end-start,3)
		if c.getinfo(pycurl.HTTP_CODE) == 200:
			print "time: {} ThreadID {} start {} end {} takes {} s".format(delay_key, ThreadID, start, end, diff)
			result.put([delay_key,ThreadID,round(end-start,3)])
		else:
			print "ThreadID {} query http code error {}".format(ThreadID,pycurl.HTTP_CODE)
			error_log.put([delay_key,str(pycurl.HTTP_CODE)])
		c.close()
	except pycurl.error , error:
		print "ThreadID {} query pycurl.error {}".format(ThreadID,error)
		error_log.put([delay_key,str(error)])
	except IOError ,error:
		print "ThreadID {} query IOError {}".format(ThreadID,error)
		error_log.put([delay_key,str(error)])
	except Exception,error:
		print "ThreadID {} query Exception {}".format(ThreadID,error)
		error_log.put([delay_key,str(error)])


def plot_by_threadID(): # unused for now
	# plot by threadID
	plot_data = sorted(result,key = lambda x: (x[0], x[1])) # sort by delay key and threadid
	pyplot.figure(1)
	pyplot.plot([d[2] for d in plot_data],linewidth=2.0) # plot the response time
	pyplot.ylabel('response time in seconds')
	pyplot.xlabel('ThreadID')
	pyplot.title('{} results avg_users: {} for {} seconds by ThreadID'.format(mode_name, avg_users,sample_size))
	figname = str('result_'+group_name+'_'+test_name+'_'+mode_name+'_'+time.strftime("_%Y_%m_%d_%H_%M_%S"))
	pyplot.savefig(os.path.join(current_dir,figname))

def create_batch(delay,batch_size,ThreadID,open_url,request_timeout,username=None,password=None):
	
	for i in range(batch_size):
		try:
			#t = threading.Timer(delay, query,(delay,ThreadID,open_url,username,password,request_timeout)) # (delay and function)
			t = threading.Thread(target = query,args= (delay,ThreadID,open_url,username,password,request_timeout,)) 
			t.start() # Start new Threads
			batch.put(t)
			ThreadID += 1

		except Exception,error:
			print "create_batch error {}".format(error)
	return ThreadID

def group_testing(group_id,group_holder,request_timeout):
	print "processing group id {}".format(group_id) #new group
	group_output = []
	for i in group_holder[group_id]: # witin in group
		'''	
		global result
		global batch
		global error_log
		result = []
		batch = []
		error_log = []
		'''


		with result.mutex:
			result.queue.clear()
		with batch.mutex:
			batch.queue.clear()
		with error_log.mutex:
			error_log.queue.clear()


		ThreadID = 0


		avg_users = None
		test_name = i['test_name']
		mode = i['mode']
		sample_size = i['sample_size']
		open_url = i['url']
		group_name = i['group_name']
		username = i['username']
		password = i['password']
		server_name = i['server_name']
		city = i['city']
		trip_time = i['trip_time']
		
		
		if mode == 2 or mode == 3: avg_users = i['avg_users']
		mode_name = None
		sequence = None
		total_requests = 0
		
		if mode == 0 and i['check']:
			sequence = single_thread(sample_size)
			mode_name = 'single_thread'
		elif mode == 1 and i['check']:
			sequence = multi_thread(sample_size)
			mode_name= 'multi_thread'
		elif mode == 2 and i['check']:
			avg_users = i['avg_users']
			mode_name = 'poisson_dist'
			sequence = poisson_dist(group_name,test_name,mode_name,avg_users,sample_size)
		elif mode == 3 and i['check']:
			mode_name = 'normal_dist'
			avg_users = i['avg_users']
			std = sample_mean/4.0 # default
			sequence = normal_dist(avg_users,std,sample_size)
		else:
			mode == None
			print "mode {} is invalid, please check your mode for group: {} test_name: {} ".format(mode, group_name,test_name)
	
		if mode:
			print "starting test on {} {} sample_size {}".format(test_name,mode_name,sample_size)
			timecounter = 0
			for delay, batch_size in enumerate(sequence):
				time_granularity = 1
				if timecounter == delay:
					if username and password:
						ThreadID = create_batch(delay,batch_size,ThreadID,open_url,request_timeout,username,password)
					else:
						ThreadID = create_batch(delay,batch_size,ThreadID,open_url,request_timeout)
					total_requests += batch_size
					timecounter += 1
				time.sleep(time_granularity) # in seconds
					
			for k in list(batch.queue):
				k.join() # wait until all threads finish
			
			result_list = list(result.queue) # convert Queue to list
			error_log_list = list(error_log.queue)
			# result and error_log are global variables 
			result_num = len(result_list) # test result

			error_count = unique_count(error_log_list) # find error_key,counts
			error_sum = unique_count(error_log_list, index=1)
			error_bool = bool(error_count)
			if result_num:
				latency_avg = np.mean(result_list,axis=0)
				latency_max = np.max(result_list,axis=0)
				latency_min = np.min(result_list,axis=0)
				latency_std = np.std(result_list,axis=0)
	
			od_output = (
				("group_name",group_name),
				("test_name",test_name),
				('server_name',server_name),
				('city', city),
				('trip_time', trip_time),
				('avg_users', avg_users),
				("mode_name",mode_name),
				("sample_size",sample_size),
				("total_requests",total_requests),
				("ok_requests",result_num),
				("latency_avg",round(latency_avg[2],3) if result_num else 0),
				("latency_max",round(latency_max[2],3) if result_num else 0),
				("latency_min",round(latency_min[2],3) if result_num else 0),
				("latency_std",round(latency_std[2],3) if result_num else 0),
				("error_summary",error_sum if error_bool else 0),
				("error_rate",round(float(total_requests-result_num)/total_requests,3) if error_bool else 0),
				("error_type",set([e[1] for e in error_log_list]) if error_bool else 0),
				("url",open_url)
			)
			od_output = collections.OrderedDict(od_output)
			group_output.append(od_output)

			x= []
			y= []
			avg_result = result_count(result_list)
			for k,v in avg_result.iteritems():
				x.append(k)
				y.append(v)
			
			figname = str('group_'+group_name+'_'+test_name+'_'+mode_name+'_'+time.strftime("_%Y_%m_%d_%H_%M_%S"))
			pyplot.figure(0)
			ax = pyplot.subplot(212) #  numrows, numcols, fignum
			ax.set_title('Above testing results by avg latency'.format(mode_name, avg_users,sample_size))
			ax.set_ylabel('avg response time in seconds')
			ax.set_xlabel('testing time in seconds')
			pyplot.plot(x,y,'r--',linewidth=2.0)
			pyplot.savefig(os.path.join(current_dir,figname))

			if error_bool:
				pyplot.figure(1)
				pyplot.ylabel('num of Error requests')
				pyplot.xlabel('num of time in seconds')
				pyplot.title('Error graph for poisson_dist avg_users: {} for {} seconds'.format(avg_users,sample_size))
				figname = str('group_'+group_name+'_'+test_name+'_'+mode_name+'_Error_Graph _'+time.strftime("_%Y_%m_%d_%H_%M_%S"))
				error_x=[]
				error_y=[]
				for k,v in error_count.iteritems():
					error_x.append(k)
					error_y.append(v)

				print "error_count {}".format(error_count)
				pyplot.plot(error_x,error_y,'r--',linewidth=2.0)
				pyplot.savefig(os.path.join(current_dir,figname))
			pyplot.close("all")
			gc.collect() # enforce GC to avoid python segmentation fault
			print "{} finished!".format(i['test_name'])

			if debug:
				print "total_requests {}" .format(total_requests)
				print "ok_requests {}" .format(result_num)
				print "latency_avg {}" .format(round(latency_avg[2],3))
				print "latency_max {}" .format(latency_max[2])
				print "latency_min {}" .format(latency_min[2])
				print "latency_std {}" .format(round(latency_std[2],3))
				if len(error_count):
					print "error_summary {}".format(error_sum)
					error_rate = float(total_requests-result_num)/total_requests
					print "error_rate {}".format(error_rate)
					print "type of error {}".format(set(error_log_list))

			
			rest_time = 20  # give time in second to let server finish previous requests before next test
			print "waiting for next round sleep {} seconds ".format(rest_time)
			time.sleep(rest_time)
	return group_output
				
if __name__ == "__main__":
	input_filename = 'stress_input.csv'  # default input files
	request_timeout = 60  # default request_timeout
	if len(sys.argv) == 2:
		print 'Number of arguments:', len(sys.argv), 'arguments.'
		print 'Argument List:', str(sys.argv)
		input_filename = sys.argv[1] # city input files
	elif len(sys.argv) == 3:
		input_filename = sys.argv[1] # default input files
		request_timeout = int(sys.argv[2]) # default request_timeout
	
	input_data = read_input(input_filename) # default inputfile name is stress_input.csv
	input_data = all_validation(input_data,request_timeout)

	if debug: 
		print "all_validation done!"

	group_holder = collections.OrderedDict()
	for data in input_data:
		if data['group_name'] in group_holder:
			group_holder[data['group_name']].append(data)
		else:
			group_holder[data['group_name']] = []
			group_holder[data['group_name']].append(data)

	if not os.path.isdir(current_dir): 
		os.makedirs(current_dir) # create a new folder for each set of test

	summary = []
	for group_id in group_holder:
		group_output_data = group_testing(group_id,group_holder,request_timeout) #start testing for each group
		group_output_file = 'group_'+str(group_id)+'_stress_output.csv'
		each_group_sum = write_output(group_output_data,file_name=group_output_file) # produce quick report for each group 
		summary.append(each_group_sum)
	figname = str('stress_output_sum_'+time.strftime("_%Y_%m_%d_%H_%M_%S")+'.csv')
	summary_report(summary,figname) # produce quick summary report for all group 


	




'''
	Author : Sumanth Prabhu <sumanthprabhu.104@gmail.com>  
'''

from bs4 import BeautifulSoup

import urllib2
import re
import time
import hashlib
import robotparser

class Crawler:
	''' The Crawler class '''

	def __init__(self, url):
		''' Initialize URL queue '''

		self.url_queue = [url]
		self.hosts = {}
		self.bloomfilter = [0] * 512

	def de_ja_vu(self, url):

		''' Checks if URL is already parsed
		
			number of hash functions = 3
			number of URLs = 100
			number of bits = 512

			Probability of a false positive = (1 - e ^ (-3 * 100 / 512)) ** 3
											= 0.0872
		'''

		h1 = int(hashlib.sha256(url).hexdigest(), base=16) % 512
		h2 = int(hashlib.sha384(url).hexdigest(), base=16) % 512
		h3 = int(hashlib.sha512(url).hexdigest(), base=16) % 512
		
		
		if self.bloomfilter[h1] == 1 and self.bloomfilter[h2] == 1 and\
		self.bloomfilter[h3] == 1 : 
			# Already visited
			return True
		
		self.bloomfilter[h1] = 1
		self.bloomfilter[h2] = 1
		self.bloomfilter[h3] = 1

		return False


	def extract_host(self, url):
		''' Extract the host '''

		#extract host
		host = re.match(r'https?://[^/]+((?=/)|$)', url)
		host = host.group()
		
		if host == url:
			#path is empty
			path = "/"
		
		else:
			path = url[len(host) + 1:]

		return (host, path)


	def md5_generate(self, host, path):
		''' Generate md5 '''
		
		m1 = hashlib.md5()
		m1.update(host)
		m1 = m1.hexdigest()

		
		m2 = hashlib.md5()
		m2.update(host + path)
		m2 = m2.hexdigest()
		
		return (m1, m2)


	def is_safe(self, url, host, path, md5_host, md5_url):
		''' Check if bot is allowed by that host'''

		found = self.de_ja_vu(url)
		
		if found: 
			#URL has been parsed before do not parse the URL
			msg = url + " has been parsed previously"

			return (False, msg)

		#robots.txt file parser
		rp = robotparser.RobotFileParser()

		#check if value is present in hosts dict
		if md5_host in self.hosts:

			#identify if URL is disallowed
			paths = self.hosts[md5_host]
			found = [md5_host.group() for p in paths 
				for md5_host in [re.search(p, path)] if md5_host]

			if found:
				#No permission to fetch
				return (False, "Not permitted to fetch " + url)
			
			else:
				#hit the path /robots.txt
				try:
					
					rp.set_url(host + "/robots.txt")
					rp.read()

					# "*" represents the user-agent here
					allowed = rp.can_fetch("*", url)

					if allowed :
						return (allowed, ) #True
					else:
						#add to dictionary
						self.hosts[md5_host] = self.hosts.get(md5_host, [])
						self.hosts[md5_host].append(path)
						
						return (allowed, "Not permitted to fetch " + url)
				except:
					return (False, "Not permitted to fetch " + url)

		else:
			#hit the path /robots.txt
			try:

				rp.set_url(host + "/robots.txt")
				rp.read()

				# "*" represents the user-agent here
				allowed = rp.can_fetch("*", url)

				if allowed :
					return (allowed, ) #True
				else:
					#add to dictionary
					self.hosts[md5_host] = self.hosts.get(md5_host, [])
					self.hosts[md5_host].append(path)
					
					return (allowed, "Not permitted to fetch " + url)
			except:
				return (False, )


	def crawl(self, limit = 10):
		''' 
		Parse URLs in the queue one by one 
		Max number of URLs is equal to "limit"
		'''
		crawl_count = 0

		print "Crawling..."

		#Parse content from next URL in the queue
		next_url = self.url_queue.pop(0)
			
		#extract host and path
		host, path = self.extract_host(next_url)
			
		#md5 host and URL
		md5_host, md5_url = self.md5_generate(host, path)

		#check if the host allows bots
		response = self.is_safe(next_url, host, path, md5_host, md5_url)

		if not response[0]:
			print response[1]
			return

		self.url_queue.append(next_url)

		while len(self.url_queue) > 0 and crawl_count < limit:

			#Parse content from next URL in the queue
			next_url = self.url_queue.pop(0)
			
			#increment number of URLS crawled	
			crawl_count += 1

			try:
				request = urllib2.Request(next_url)
				opener = urllib2.build_opener()
				response = opener.open(request).read()
				print "Fetched : " + next_url

			except urllib2.URLError:
				continue #invalid URL

			soup = BeautifulSoup(response)
			anchortags = soup.findAll('a')

			#identify useful URLS
			for tag in anchortags:

				try:
					href = tag['href']
				
				except KeyError:
					#no href attribute
					#continue to next URL
					continue 
				
				except socket.error:
					pass #Temporary connection issue

				if href.endswith(('.msi','.bz2','.zip')):
					# skip such URLs
					continue 

				if href.startswith('http'):
					#check if the host allows bots

					#extract host and path
					host, path = self.extract_host(href)
				
					#md5 host and URL
					md5_host, md5_url = self.md5_generate(host, path)
					
					response = self.is_safe(href, host, path, 
												md5_host, md5_url)

					if response[0]:
						if (crawl_count + len(self.url_queue)) <= limit:
							#push URL to queue
							print "Pushing " + href + " to the queue"
							self.url_queue.append(href)
						
						else:
							break

				else:
					#Partial URL

					#next_url contains the URL currently being parsed
					current_url = next_url

					#prepend the base URL to the partial URL
					#remove the extra '/'
					
					if current_url[-1] == '/':
						current_url += href[1:]
					else:
						current_url += href[:]

					#extract host and path
					host, path = self.extract_host(current_url)
				
					#md5 host and URL
					md5_host, md5_url = self.md5_generate(host, path)

					response = self.is_safe(current_url, host, path, 
											md5_host, md5_url)
			
					if response[0]:
						if (crawl_count + len(self.url_queue)) <= limit:
							#push URL to queue
							print "Pushing " + current_url + " to the queue"
							self.url_queue.append(current_url)
						
						else:
							break

		#done crawling
		print "Crawling completed"
		print "Fetched %d links" % crawl_count


def valid(url):
	''' Check validity of URL '''
	
	try:
		urllib2.urlopen(url)
		return True
	
	except Exception as e:
		return False


def main():
	start = time.time()

	url = raw_input("Enter the seed URL... ")
	
	if not valid(url):
		print "Seems to be an invalid URL.."
	
	else:
		c = Crawler(url)
		
		#set limit to 100
		c.crawl(100)
	
	end = time.time()

	print "Time taken = %ds" % (end - start)


if __name__ == "__main__":
	main()	

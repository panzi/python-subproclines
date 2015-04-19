import select
import subprocess
from select import POLLIN, POLLHUP, EPOLLIN, EPOLLHUP
from fcntl import fcntl, F_GETFL, F_SETFL
from os import O_NONBLOCK, read
from collections import defaultdict

BUFSIZ = 1024 * 4

def xpoll_parallel_reader(streams, poll, POLLIN, POLLHUP, buffer_size=BUFSIZ):
	if buffer_size < 1:
		raise ValueError("buffer size must be >= 1")

	fds = []
	fd_map = {}

	for i, stream in enumerate(streams):
		fd = stream.fileno()

		fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK)

		poll.register(fd, POLLIN)
		fds.append(fd)
		fd_map[fd] = i

	while True:
		got_data = False
		for fd, events in poll.poll():
			if events & POLLIN:
				chunk = read(fd, buffer_size)
				if chunk:
					got_data = True
					yield fd_map[fd], chunk

		if not got_data:
			break

def epoll_parallel_reader(streams,buffer_size=BUFSIZ):
	try:
		poll = select.epoll()
		for item in xpoll_parallel_reader(streams, poll, EPOLLIN, EPOLLHUP, buffer_size):
			yield item
	finally:
		poll.close()

def poll_parallel_reader(streams,buffer_size=BUFSIZ):
	return xpoll_parallel_reader(streams, select.poll(), POLLIN, POLLHUP, buffer_size)

def select_parallel_reader(streams,buffer_size=BUFSIZ):
	rlist = []
	wlist = []
	xlist = []
	fd_map = {}

	for i, stream in enumerate(streams):
		fd = stream.fileno()

		fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK)
		rlist.append(fd)
		fd_map[fd] = i

	while True:
		ravail, wavail, xavail = select.select(rlist, wlist, xlist)

		got_data = False
		for fd in ravail:
			chunk = read(fd, buffer_size)
			if chunk:
				got_data = True
				yield fd_map[fd], chunk

		if not got_data:
			break

if hasattr(select,'epoll'):
	parallel_reader = epoll_parallel_reader
elif hasattr(select,'poll'):
	parallel_reader = poll_parallel_reader
else:
	parallel_reader = select_parallel_reader

def lines(parallel_streams):
	buffers = defaultdict(list)
	
	for i, chunk in parallel_streams:
		buf = buffers[i]
		pos = chunk.find(b'\n')

		if pos < 0:
			if chunk:
				# no newline, just more data
				buf.append(chunk)
			elif buf:
				# detected unterminated line at EOF
				yield i, b''.join(buf)
				del buf[:]
		else:
			end = pos + 1
			if buf:
				# build buffered line
				buf.append(chunk[:end])
				yield i, b''.join(buf)
				del buf[:]
			else:
				# nothing previously buffered
				yield i, chunk[:end]

			# scan for more lines in rest of chunk
			n = len(chunk)
			j = end

			while j < n:
				pos = chunk.find(b'\n',j)

				if pos < 0:
					# no more newlines, rest needs to be buffered
					buf.append(chunk[j:])
					break

				# one more line from chunk
				end = pos + 1
				yield i, chunk[j:end]
				j = end

	# unterminated lines at EOF:
	for i in buffers:
		buf = buffers[i]
		if buf:
			yield i, b''.join(buf)

STDOUT = 0
STDERR = 1

def subprocchunks(args,buffer_size=BUFSIZ):
	proc = subprocess.Popen(args,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
	return parallel_reader([proc.stdout,proc.stderr],buffer_size)

def subproclines(args,buffer_size=BUFSIZ):
	return lines(subprocchunks(args,buffer_size))

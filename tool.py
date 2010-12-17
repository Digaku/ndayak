
import sys
from socket import *

def usage():
    print "Usage:"
    print "      %s add|rm [current-ndayak-addr] [ndayaks-addr]" % sys.argv[0]


if len(sys.argv) < 3:
    usage()
    exit(1)

cmd = sys.argv[1]

if cmd == "add":
	curr = sys.argv[2]
	rem = sys.argv[3]
	assert curr
	assert rem
	addr = rem.split(":")
	assert len(addr) > 1
	addr[1] = int(addr[1])
	addr = tuple(addr)
	s = socket(AF_INET, SOCK_DGRAM)
	s.sendto("/srv %s up" % curr,addr)
	print "/srv %s up" % curr
	s.close()
	
elif cmd == "rm":
	curr = sys.argv[2]
	rem = sys.argv[3]
	assert curr
	assert rem
	addr = rem.split(":")
	assert len(addr) > 1
	addr[1] = int(addr[1])
	addr = tuple(addr)
	s = socket(AF_INET, SOCK_DGRAM)
	s.sendto("/srv %s down" % curr,addr)
	s.close()





include $(GOROOT)/src/Make.inc

TARG=ndayak
GOFILES=\
	core.go \
	stats.go \
	log.go


include $(GOROOT)/src/Make.pkg

CC=$(GOBIN)/6g 
LD=$(GOBIN)/6l 

OBJS =\
	simpleconfig.6 \
	worker.6 \
	main.6

exe: $(OBJS)
	$(LD) -o $(TARG) main.6

%.6 : %.go 
	$(CC) $< 

% : %.6 
	$(LD) -L . -o $@ $^ 

clean-ndayak:
	rm *.6
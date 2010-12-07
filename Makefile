CC=$(GOBIN)/6g 
LD=$(GOBIN)/6l 

OBJS =\
	core.6 \
	main.6

all: dgqem

dgqem: $(OBJS)
	$(LD) -o $@ main.6

%.6 : %.go 
	$(CC) $< 

% : %.6 
	$(LD) -L . -o $@ $^ 

clean:
	rm *.6
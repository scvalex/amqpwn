FRAMMING_DATA := Network/AMQP/FrammingData.hs

all: framing

framing: $(FRAMMING_DATA)

clean:
	rm -f $(FRAMMING_DATA)

.PHONY: all framing clean

$(FRAMMING_DATA): Tools/Builder.hs Tools/amqp0-8.xml
	runhaskell $+ > $@

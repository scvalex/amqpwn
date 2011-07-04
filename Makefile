FRAMING_DATA := Network/AMQP/FramingData.hs

all: build

framing: $(FRAMING_DATA)

build: dist/setup-config framing
	cabal build

clean:
	cabal clean
	rm -f $(FRAMING_DATA)

test: build
	runhaskell Test/Simple.hs

doc: build
	cabal haddock --internal

.PHONY: all framing clean test doc

dist/setup-config: amqp.cabal
	cabal configure

$(FRAMING_DATA): Codegen/Codegen.hs Codegen/amqp0-9-1.xml
	runhaskell $+ > /tmp/Framing.hs && cp /tmp/Framing.hs $@

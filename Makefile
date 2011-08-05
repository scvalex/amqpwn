FRAMING_DATA := Network/AMQP/FramingData.hs

all: build

build: dist/setup-config
	cabal build

clean:
	cabal clean
	rm -f $(FRAMING_DATA)

test: build
	runhaskell Test/Simple.hs

doc: build
	cabal haddock --internal

.PHONY: all clean test doc

dist/setup-config: amqpwn.cabal
	cabal configure

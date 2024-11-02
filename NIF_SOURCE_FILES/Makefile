# Makefile

CXX = gcc
CXXFLAGS = -fPIC -shared -O3 -std=c23
ERL_CFLAGS = $(shell erl -noshell -eval 'io:format("~s", [code:root_dir()])' -s init stop)/usr/include
ERL_LIBDIR = $(shell erl -noshell -eval 'io:format("~s/lib", [code:root_dir()])' -s init stop)
ERL_LDFLAGS = -L$(ERL_LIBDIR) -lei

NIF_NAME = financial_data_preparation
TARGET = ../priv/$(NIF_NAME).so

SRC = financial_data_preparation_nif.c cumulative_sum_with_reset.c information_driven_bars.c

all: $(TARGET)

$(TARGET): $(SRC)
	mkdir -p ../priv
	$(CXX) $(CXXFLAGS) -I $(ERL_CFLAGS) -o $(TARGET) $(SRC) $(ERL_LDFLAGS)

clean:
	rm -f $(TARGET)

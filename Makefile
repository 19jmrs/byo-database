# Compiler
CC = gcc

# Compiler flags
CFLAGS = -Wall -Wextra -std=c11

# Executable name
TARGET = main

all: $(TARGET)

# Rule to compile main.c into an executable
$(TARGET): main.c
	$(CC) $(CFLAGS) -o $(TARGET) main.c

run: $(TARGET)
	./$(TARGET)

clean:
	del /F /Q $(TARGET).exe || rm -f $(TARGET)
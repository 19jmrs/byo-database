#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <io.h>
#include <string.h>

typedef enum{
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum{
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef struct{
    StatementType type;
} Statement;

typedef struct{
    char *buffer;
    size_t buffer_length;
    size_t input_length; 
} InputBuffer;

InputBuffer *new_input_buffer(){
    InputBuffer *input_buffer = (InputBuffer *)malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;

    return input_buffer;
}

void print_prompt(){printf("db > ");}

// Safe input reading function that handles arbitrary input lengths
void read_input(InputBuffer *input_buffer) {
    const size_t CHUNK_SIZE = 1024;
    size_t total_bytes = 0;
    char *current_position;
    bool more_data = true;

    if (input_buffer->buffer == NULL) {
        input_buffer->buffer = (char *)malloc(CHUNK_SIZE);
        if (input_buffer->buffer == NULL) {
            fprintf(stderr, "Error: Memory allocation failed\n");
            exit(EXIT_FAILURE);
        }
        input_buffer->buffer_length = CHUNK_SIZE;
    }

    // Reset buffer for new input
    memset(input_buffer->buffer, 0, input_buffer->buffer_length);
    current_position = input_buffer->buffer;

    while (more_data) {
        // Check if we need more space
        if (total_bytes + CHUNK_SIZE > input_buffer->buffer_length) {
            size_t new_size = input_buffer->buffer_length * 2;
            char *new_buffer = (char *)realloc(input_buffer->buffer, new_size);
            if (new_buffer == NULL) {
                fprintf(stderr, "Error: Memory reallocation failed\n");
                free(input_buffer->buffer);
                exit(EXIT_FAILURE);
            }
            // Update pointers after realloc
            current_position = new_buffer + total_bytes;
            input_buffer->buffer = new_buffer;
            input_buffer->buffer_length = new_size;
        }

        // Read chunk
        if (fgets(current_position, CHUNK_SIZE, stdin) == NULL) {
            if (total_bytes == 0) {
                fprintf(stderr, "Error: Failed to read input\n");
                exit(EXIT_FAILURE);
            }
            break;
        }

        size_t chunk_length = strlen(current_position);
        total_bytes += chunk_length;

        // Check if we got a newline
        if (current_position[chunk_length - 1] == '\n') {
            current_position[chunk_length - 1] = '\0'; // Remove newline
            total_bytes--; // Adjust for removed newline
            more_data = false;
        } else {
            current_position += chunk_length;
        }
    }

    input_buffer->input_length = total_bytes;
}

void close_input_buffer(InputBuffer *input_buffer){
    free(input_buffer->buffer);
    free(input_buffer);
}
//commands like ".exit" that are not SQL commands
MetaCommandResult do_meta_command(InputBuffer *input_buffer){
    if(strcmp(input_buffer->buffer, ".exit") == 0){
        exit(EXIT_SUCCESS);
    } else{
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement){
    //use strncmp in this case because insert will be followed by more data
    if(strncmp(input_buffer->buffer, "insert", 6) == 0){
        statement->type = STATEMENT_INSERT;
        return PREPARE_SUCCESS;
    }
    if(strcmp(input_buffer->buffer, "select") == 0){
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement(Statement *statement){
    switch(statement->type){
        case STATEMENT_INSERT:
            printf("This is where we would do an insert. \n");
            break;
        case STATEMENT_SELECT:
            printf("This is where we would do a select. \n");
            break;
    }
}

int main(int argc, char *argv[])
{
    InputBuffer *input_buffer = new_input_buffer();
    while(true){
      print_prompt();
      read_input(input_buffer);
      
      //check if it starts with '.', if yes than it is a meta keyword
      if(input_buffer->buffer[0] == '.'){
        switch (do_meta_command(input_buffer))
        {
            case META_COMMAND_SUCCESS:
                continue;            
            case META_COMMAND_UNRECOGNIZED_COMMAND:
                printf("Unrecognized command '%s \n", input_buffer->buffer);
                continue;
        }
      }
      //process as statement if not a meta command
      Statement statement;
      switch (prepare_statement(input_buffer, &statement))
      {
        case PREPARE_SUCCESS:
            break;
        case PREPARE_UNRECOGNIZED_STATEMENT:
            printf("Unrecognized keyword at start of '%s'. \n", input_buffer->buffer);
            continue;
      }

      execute_statement(&statement);
      printf("Executed. \n");
    }
}

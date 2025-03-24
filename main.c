#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <io.h>
#include <string.h>
#include <stdint.h>

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)
#define TABLE_MAX_PAGES 100
typedef struct {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE];
    char email[COLUMN_EMAIL_SIZE];
} Row;

// should change to #define instead of const?
#define ID_SIZE size_of_attribute(Row, id)
#define USERNAME_SIZE size_of_attribute(Row, username)
#define EMAIL_SIZE  size_of_attribute(Row, email)
#define ID_OFFSET 0
#define USERNAME_OFFSET (ID_OFFSET + ID_SIZE)
#define EMAIL_OFFSET (USERNAME_OFFSET + USERNAME_SIZE)
#define ROW_SIZE (ID_SIZE + USERNAME_SIZE + EMAIL_SIZE)

#define PAGE_SIZE 4096
#define ROWS_PER_PAGE (PAGE_SIZE/ROW_SIZE)
#define TABLE_MAX_ROWS  (ROWS_PER_PAGE * TABLE_MAX_PAGES)

//https://cstack.github.io/db_tutorial/parts/part4.html

typedef struct{
    uint32_t num_rows;
    void *pages[TABLE_MAX_PAGES];
} Table;


typedef enum{
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum{
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT,
    PREPARE_SYNTAX_ERROR
} PrepareResult;

typedef enum{
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL,
} ExecuteResult;
typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef struct{
    StatementType type;
    Row row_to_insert;
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

void print_row(Row *row){
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void *row_slot(Table *table, uint32_t row_num){
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void *page = table->pages[page_num];
    if(page == NULL){
        //allocate memory when trying to access page
        page = table->pages[page_num] = malloc(PAGE_SIZE);
    }
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}


void serialize_row(Row *source, void *destination){
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void *source, Row *destination){
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement){
    //use strncmp in this case because insert will be followed by more data
    if(strncmp(input_buffer->buffer, "insert", 6) == 0){
        statement->type = STATEMENT_INSERT;
        //read arguments
        int args_assigned = sscanf(input_buffer->buffer, "insert %d %s %s",
            &(statement->row_to_insert.id), statement->row_to_insert.username, statement->row_to_insert.email);
        if(args_assigned < 3){
            return PREPARE_SYNTAX_ERROR;
        }
        return PREPARE_SUCCESS;
    }
    if(strcmp(input_buffer->buffer, "select") == 0){
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_insert(Statement *statement, Table *table){
    if(table->num_rows >= TABLE_MAX_ROWS){
        return EXECUTE_TABLE_FULL;
    }
    Row *row_to_insert = &(statement->row_to_insert);

    serialize_row(row_to_insert, row_slot(table, table->num_rows));
    table->num_rows += 1;

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement *statement, Table *table){
    Row row;
    for(uint32_t i = 0; i < table->num_rows; i++){
        deserialize_row(row_slot(table, i), &row);
        print_row(&row);
    }
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement *statement, Table *table){
    switch (statement->type)
    {
        case STATEMENT_INSERT:
            return execute_insert(statement, table);
        case STATEMENT_SELECT:
            return execute_select(statement, table);
    }
}

Table *newTable(){
    Table *table = (Table *)malloc(sizeof(Table));
    table->num_rows = 0;
    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++){
        table->pages[i] = NULL;
    }
    return table;
}

void freeTable(Table *table){
    for(int i = 0; table->pages[i]; i++){
        free(table->pages[i]);
    }
    free(table);
}

void close_input_buffer(InputBuffer *input_buffer){
    free(input_buffer->buffer);
    free(input_buffer);
}

//commands like ".exit" that are not SQL commands
MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table *table){
    if(strcmp(input_buffer->buffer, ".exit") == 0){
        close_input_buffer(input_buffer);
        freeTable(table);
        exit(EXIT_SUCCESS);
    } else{
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

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

int main(int argc, char *argv[])
{
    Table *table = newTable();
    InputBuffer *input_buffer = new_input_buffer();
    while(true){
      print_prompt();
      read_input(input_buffer);
      
      //check if it starts with '.', if yes than it is a meta keyword
      if(input_buffer->buffer[0] == '.'){
        switch (do_meta_command(input_buffer, table))
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
        case PREPARE_SYNTAX_ERROR:
            printf("Syntax error. Could not parse statement. \n");
            continue;
        case PREPARE_UNRECOGNIZED_STATEMENT:
            printf("Unrecognized keyword at start of '%s'. \n", input_buffer->buffer);
            continue;
      }

      switch(execute_statement(&statement, table)){
        case EXECUTE_SUCCESS:
            printf("Executed. \n");
            break;
        case EXECUTE_TABLE_FULL:
            printf("Error: Table full. \n");
            break;
      }
    }
}

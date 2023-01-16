#include <errno.h>
#include <fcntl.h>
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>
#define _WIN32

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)
#define TABLE_MAX_PAGES 100


// Enums
typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;


typedef enum {
  PREPARE_SUCCESS,
  PREPARE_SYNTAX_ERROR,
  PREPARE_NEGATIVE_ID,
  PREPARE_UNRECOGNIZED_STATEMENT,
  PREPARE_STRING_TOO_LONG
 } PrepareResult;

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef enum {
    EXECUTE_SUCCESS,
    EXECUTE_DUPLICATE_KEY,
    EXECUTE_TABLE_FULL
} ExecuteResult;

typedef enum {
    NODE_INTERNAL,
    NODE_LEAF
 } NodeType;

// Structs

// Implement InputBuffer Wrapper
typedef struct {
    char* buffer;
    size_t buffer_len;
    size_t input_len;
} InputBuffer;

typedef struct {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1]; // Add additional byte for null character
                                            //          |
    char email[COLUMN_EMAIL_SIZE + 1];     //       <---|  
} Row;

typedef struct {
    StatementType type;
    Row row_to_insert; // Used only by the "insert" command
} Statement;

// This structure will locate a certain block of memory and return it
typedef struct {
    FILE* file_descriptor;
    uint32_t file_length;
    uint32_t num_pages;
    void* pages[TABLE_MAX_PAGES];
} Pager;

// Let's get a table structure to print to pages of rows. This will keep track of how many rows exist
typedef struct {
    uint32_t root_page_num; // A B-Tree is identified by its root node number
    Pager* pager;
} Table;

typedef struct {
    Table* table;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_table; // Indicates the next position past the last element
} Cursor;

// More definitions
#define ID_SIZE size_of_attribute(Row, id)
#define USERNAME_SIZE size_of_attribute(Row, username)
#define EMAIL_SIZE size_of_attribute(Row, email)
#define ID_OFFSET 0
#define USERNAME_OFFSET (ID_OFFSET + ID_SIZE)

// Constants
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const uint32_t PAGE_SIZE = 4096;
#define ROWS_PER_PAGE (PAGE_SIZE / ROW_SIZE)
#define TABLE_MAX_ROWS (ROWS_PER_PAGE * TABLE_MAX_PAGES)


/*

Important B-Tree Code

*/

/* 

Common Node Metadata (In B-Tree)

*/

const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = sizeof(uint8_t);
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = sizeof(uint8_t) + sizeof(uint8_t);
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
#define COMMON_NODE_METADATA_SIZE NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE


/* 

Leaf Node Metadata

    - Needs to store number of "cells"
    - A cell is a key-value pair

*/

const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
#define LEAF_NODE_NUM_CELLS_OFFSET COMMON_NODE_METADATA_SIZE
#define LEAF_NODE_NEXT_LEAF_OFFSET LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE
#define LEAF_NODE_METADATA_SIZE COMMON_NODE_METADATA_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE

/*

Leaf Node Body Format
    - The body of a leaf node is an array of cells
    - Each cell is a key followed by a value (i.e. a serialized table row)

*/
#define LEAF_NODE_KEY_SIZE sizeof(uint32_t)
#define LEAF_NODE_KEY_OFFSET 0
#define LEAF_NODE_VALUE_SIZE ROW_SIZE
#define LEAF_NODE_VALUE_OFFSET (LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE)
#define LEAF_NODE_CELL_SIZE (LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE)
#define LEAF_NODE_SPACE_FOR_CELLS PAGE_SIZE - LEAF_NODE_METADATA_SIZE
#define LEAF_NODE_MAX_CELLS (LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE)
#define LEAF_NODE_RIGHT_SPLIT_COUNT (LEAF_NODE_MAX_CELLS + 1) / 2
#define LEAF_NODE_LEFT_SPLIT_COUNT (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT

// Internal Node Header Layout
#define INTERNAL_NODE_NUM_KEYS_SIZE sizeof(uint32_t)
#define INTERNAL_NODE_NUM_KEYS_OFFSET COMMON_NODE_METADATA_SIZE
#define INTERNAL_NODE_RIGHT_CHILD_SIZE sizeof(uint32_t)
#define INTERNAL_NODE_RIGHT_CHILD_OFFSET (INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_OFFSET)
#define INTERNAL_NODE_HEADER_SIZE (COMMON_NODE_METADATA_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE)

// Internal Node Body Format
#define INTERNAL_NODE_KEY_SIZE sizeof(uint32_t)
#define INTERNAL_NODE_CHILD_SIZE sizeof(uint32_t)
#define INTERNAL_NODE_CELL_SIZE (INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE)
#define INTERNAL_NODE_MAX_CELLS 3

// Some function declarations
void* getPage(Pager* pager, uint32_t pageNum);
void printConstants();
void serializeRow(Row* source, void* destination);
void deserializeRow(void* source, Row* destination);
NodeType getNodeType(void* node);
void setNodeType(void* node, NodeType type);
void splitLeafNodeAndInsert(Cursor* cursor, uint32_t key, Row* value);
void createNewRoot(Table* table, uint32_t rightChildPageNum);
uint32_t* internalNodeNumKeys(void* node);
uint32_t* internalNodeRightChild(void* node);
uint32_t* internalNodeChild(void* node, uint32_t childNum);
uint32_t* internalNodeKey(void* node, uint32_t keyNum);
bool isRootNode(void* node);
void setNodeRoot(void* node, bool isRoot);
void initializeInternalNode(void* node);
uint32_t getNodeMaxKey(void* node);
void print_tree(Pager* pager, uint32_t pageNum, uint32_t indentationLevel);
Cursor* internalNodeFind(Table* table, uint32_t pageNum, uint32_t key);
Cursor* tableFind(Table* table, uint32_t key);
void updateInternalNodeKey(void* node, uint32_t oldKey, uint32_t newKey);
void insertInternalNode(Table* table, uint32_t parentPageNum, uint32_t childPageNum);
uint32_t internalNodeFindChild(void* node, uint32_t key);

uint32_t* leafNodeNextLeaf(void* node) {
    return (uint32_t*) node + LEAF_NODE_NEXT_LEAF_OFFSET;
}

uint32_t* nodeParent(void* node){
    return (uint32_t*) node + PARENT_POINTER_OFFSET;
}

uint32_t getUnusedPageNum(Pager* pager) {
    return pager->num_pages;
}

uint32_t* leafNodeNumCells(void* node) {
    return (uint32_t*) node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void* leafNodeCell(void* node, uint32_t cellNum) {
    return (uint32_t*) node + LEAF_NODE_METADATA_SIZE + cellNum * LEAF_NODE_CELL_SIZE;
}

uint32_t* leafNodeKey(void* node, uint32_t cellNum) {
    return leafNodeCell(node, cellNum);
}

void* leafNodeValue(void* node, uint32_t cellNum) {
    return (uint32_t*) leafNodeCell(node, cellNum) + LEAF_NODE_KEY_SIZE;
}

void initializeLeafNode(void* node) {
    setNodeType(node, NODE_LEAF);
    setNodeRoot(node, false);
    *leafNodeNumCells(node) = 0;
    *leafNodeNextLeaf(node) = 0; // The 0 means the leaf has no siblings
}
// Function to insert key-value pairs into a leaf node
// Takes a cursor as input to represent where the pair should be inserted
void insertLeafNode(Cursor* cursor, uint32_t key, Row* value) {
    void* node = getPage(cursor->table->pager, cursor->page_num);
    uint32_t numCells = *leafNodeNumCells(node);

    if (numCells >= LEAF_NODE_MAX_CELLS) {
        // Node is full
        splitLeafNodeAndInsert(cursor, key, value);
        return;
    }

    if (cursor->cell_num < numCells) {
        // Make room for a new cell
        for(uint32_t i = numCells; i > cursor->cell_num; i--){
            memcpy(leafNodeCell(node, i), leafNodeCell(node, i - 1), LEAF_NODE_CELL_SIZE);
        }
    }

    *(leafNodeNumCells(node)) += 1;
    *(leafNodeKey(node, cursor->cell_num)) = key;
    serializeRow(value, leafNodeValue(node, cursor->cell_num));
}


// Methods
InputBuffer* newInputBuffer() {
    InputBuffer* input_buffer = (InputBuffer*) malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_len = 0;
    input_buffer->input_len = 0;

    return input_buffer;
}

void printPrompt() {
    printf("db > ");
}

void printRow(Row* row){
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void readInput(InputBuffer* input_buffer) {
    size_t bytesRead = getline(&(input_buffer->buffer), &(input_buffer->buffer_len), stdin);

    // Basic error-handling
    if (bytesRead <= 0) {
        printf("Invalid input\n");
        exit(EXIT_FAILURE);
    }

    // Ignore any trailing newlines
    input_buffer->input_len = bytesRead - 1;
    input_buffer->buffer[bytesRead - 1] = 0;
}

void closeInputBuffer(InputBuffer* buffer) {
    free(buffer->buffer);
    free(buffer);
}

// Helper function to error-check "insert" statements
PrepareResult prepareInsert(InputBuffer* buffer, Statement* statement) {
    statement->type = STATEMENT_INSERT;

    // Split each string to check its length.
    // Do this to ensure no buffer overflows are caused 
    char* keyword = strtok(buffer->buffer, " ");
    char* idString = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");

    if (idString == NULL || username == NULL || email == NULL) {
        return PREPARE_SYNTAX_ERROR;
    }

    int id = atoi(idString);

    // Check for valid ID tag
    if (id < 0) {
        return PREPARE_NEGATIVE_ID;
    }

    if (strlen(username) > COLUMN_USERNAME_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }

    if (strlen(email) > COLUMN_EMAIL_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }

    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);

    return PREPARE_SUCCESS;

}

// Our very own minimalistic "SQL Compiler"
PrepareResult prepareStatement(InputBuffer* buffer, Statement* statement) {
    if (strncmp(buffer->buffer, "insert", 6) == 0) {
        return prepareInsert(buffer, statement);
    }

    if (strcmp(buffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void serializeRow(Row* source, void* destination) {
    memcpy((uint32_t*) destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy((uint32_t*) destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy((uint32_t*) destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserializeRow(void* source, Row* destination){
    memcpy(&(destination->id), (uint32_t*) source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), (uint32_t*) source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), (uint32_t*) source + EMAIL_OFFSET, EMAIL_SIZE);
}

/* 

The method below handles the logic for missing any cached files.
Assumes pages are saved on after another with their own corresponding offset value.
If the page lies outside the bounds of the file, it'll be NULL.
From there, we can add the page to the file when the cache if flushed to disk later on.

*/
void* getPage(Pager* pager, uint32_t pageNum) {
    if (pageNum > TABLE_MAX_PAGES) {
        printf("Page number out of bounds. %d > %d\n", pageNum, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }
    // If the pager is empty
    if (pager->pages[pageNum] == NULL) {
        // Cache miss. Allocate new memory and load from file.
        void* page = malloc(PAGE_SIZE);
        uint32_t numPages = pager->file_length / PAGE_SIZE;

        // There's a possibility of a partial page being saved at end of file.
        // To prevent this, increment numPages by 1
        if (pager->file_length % PAGE_SIZE) {
            numPages += 1;
        }

        if (pageNum <= numPages) {
            fseek(pager->file_descriptor, pageNum * PAGE_SIZE, SEEK_SET);
            // size_t bytesRead = fread(pager->file_descriptor, page, PAGE_SIZE);
            // ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            size_t bytesRead = fread(page, PAGE_SIZE, pageNum * PAGE_SIZE, pager->file_descriptor);
            if (bytesRead == -1) {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }

        pager->pages[pageNum] = page;

        if (pageNum >= pager->num_pages) {
            pager->num_pages = pageNum + 1;
        }
    }

    return pager->pages[pageNum];
}

void pagerFlush(Pager* pager, uint32_t pageNum) {
    if (pager->pages[pageNum] == NULL) {
        printf("Tried to flush a null page\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = fseek(pager->file_descriptor, pageNum * PAGE_SIZE, SEEK_SET);

    if (offset == -1) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    // size_t bytesWritten = write(pager->file_descriptor, pager->pages[pageNum], PAGE_SIZE);
    size_t bytesWritten = fwrite(pager->pages[pageNum], pageNum * PAGE_SIZE, PAGE_SIZE, pager->file_descriptor);

    if (bytesWritten == -1) {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }


}

// When user exits the program, close the db connection
void dbClose(Table* table) {
    Pager* pager = table->pager;

    for(uint32_t i = 0; i < pager->num_pages; i++) {
        if (pager->pages[i] == NULL) {
            continue;
        }

        pagerFlush(pager, i);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }


    // int res = close(pager->file_descriptor); <-- Older version
    int res = fclose(pager->file_descriptor);
    if (res == -1) {
        printf("Error closing db file\n");
        exit(EXIT_FAILURE);
    }

    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        void* page = pager->pages[i];
        if(page) {
            free(page);
            pager->pages[i] = NULL;
        }
    }

    free(pager);
    free(table);
}

MetaCommandResult execMetaCommand(InputBuffer* buffer, Table* table){ 
    if (strcmp(buffer->buffer, ".exit") == 0) {
        dbClose(table);
        exit(EXIT_SUCCESS);
    } else if (strcmp(buffer->buffer, ".btree") == 0) {
        printf("Tree:\n");
        print_tree(table->pager, 0, 0);
        return META_COMMAND_SUCCESS;
    } else if (strcmp(buffer->buffer, ".constants") == 0) {
        printf("Constants:\n");
        printConstants();
        return META_COMMAND_SUCCESS;
    } else {
        return  META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

Cursor* tableStart(Table* table) {
    Cursor* cursor = tableFind(table, 0);
    void* node = getPage(table->pager, cursor->page_num);
    uint32_t numCells = *leafNodeNumCells(node);
    cursor->end_of_table = (numCells == 0);
    return cursor;
}

// Return a pointer to the position which the cursor is located
void* cursorValue(Cursor* cursor) {
    uint32_t pageNum = cursor->page_num;
    void* page = getPage(cursor->table->pager, pageNum);
    return leafNodeValue(page, cursor->cell_num);
}

Cursor* findLeafNode(Table* table, uint32_t pageNum, uint32_t key) {
    void* node = getPage(table->pager, pageNum);
    uint32_t numCells = *leafNodeNumCells(node);

    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = pageNum;

    // Search for leaf node using binary search
    uint32_t minIndex = 0;
    uint32_t onePastMaxIndex = numCells;

    while (onePastMaxIndex != minIndex) {
        uint32_t index = (minIndex + onePastMaxIndex) / 2;
        uint32_t keyAtIndex = *leafNodeKey(node, index);
        if (key == keyAtIndex) {
            cursor->cell_num = index;
            return cursor;
        }

        if (key < keyAtIndex) {
            onePastMaxIndex = index;
        } else {
            minIndex = index + 1;
        }
    }

    cursor->cell_num = minIndex;
    return cursor;
}

Cursor* tableFind(Table* table, uint32_t key) {
    uint32_t rootPageNum = table->root_page_num;
    void* rootNode = getPage(table->pager, rootPageNum);

    if (getNodeType(rootNode) == NODE_LEAF) {
        return findLeafNode(table, rootPageNum, key);
    } else {
        printf("Need to implement searching for internal nodes\n");
        exit(EXIT_FAILURE);
    }
}

void incrementCursor (Cursor* cursor) {
    uint32_t pageNum = cursor->page_num;
    void* node = getPage(cursor->table->pager, pageNum);
    cursor->cell_num += 1;
    if (cursor->cell_num >= (*leafNodeNumCells(node))) {
        cursor->end_of_table = true;
    }
}

// Makeshift "virtual machine"
ExecuteResult executeInsert(Statement* statement, Table* table) {
    Row* rowToInsert = &(statement->row_to_insert);
    uint32_t keyToInsert = rowToInsert->id;
    Cursor* cursor = tableFind(table, keyToInsert);

    void* node = getPage(table->pager, table->root_page_num);
    uint32_t numCells = *leafNodeNumCells(node);

    if (cursor->cell_num < numCells) {
        uint32_t keyAtIndex = *leafNodeKey(node, cursor->cell_num);
        if (keyAtIndex == keyToInsert) {
            return EXECUTE_DUPLICATE_KEY;
        }
    }

    // serializeRow(rowToInsert, rowSlot(table, table->num_rows));
    insertLeafNode(cursor, rowToInsert->id, rowToInsert);
    
    return EXECUTE_SUCCESS;
}

ExecuteResult executeSelect(Statement* statement, Table* table) {
    Cursor* cursor = tableStart(table);
    Row row;
    // for (uint32_t i = 0; i < table->num_rows; i++) {
    //     deserializeRow(rowSlot(table, i), &row);
    //     printRow(&row);
    // }

    while (!(cursor->end_of_table)) {
        deserializeRow(cursorValue(cursor), &row);
        printRow(&row);
        incrementCursor(cursor);
    }

    free(cursor);

    return EXECUTE_SUCCESS;
}

ExecuteResult executeStatement(Statement* statement, Table* table) {
    switch (statement->type) {
        case (STATEMENT_INSERT):
            return executeInsert(statement, table);
            break;
        case (STATEMENT_SELECT):
            return executeSelect(statement, table);
    }
}

Pager* pagerOpen(const char* filename) {
    // int fileDescriptor = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
    FILE* stream;
    //  int fd = open(filename,
    //             O_RDWR |      // Read/Write mode
    //                 O_CREAT,  // Create file if it does not exist
    //             S_IWRITE|     // User write permission
    //                 S_IREAD   // User read permission
    //             );

    FILE* fd = fopen(filename, "r+");
    // FILE* fd = fdopen();

    if(!fd) {

        fd = fopen(filename, "w+");

        if (!fd) {
            printf("Unable to open file\n");
            exit(EXIT_FAILURE);
        }
    }

    // off_t fileLength = lseek(fd, 0, SEEK_END); // <-- Older version
    off_t fileLength = fseek(fd, 0, SEEK_END);

    Pager* pager = malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = fileLength;
    pager->num_pages = (fileLength / PAGE_SIZE);

    if (fileLength % PAGE_SIZE != 0) {
        printf("DB file is not a whole number of pages. Corrupt file detected\n");
        exit(EXIT_FAILURE);
    }

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        pager->pages[i] = NULL;
    }

    return pager;
}

// Initialize and open new database file 
Table* dbOpen(const char* filename) {   
    Pager* pager = pagerOpen(filename);

    Table* table = (Table*)malloc(sizeof(Table));
    table->pager = pager;
    table->root_page_num = 0;
    
    if (pager->num_pages == 0) {
        // New DB file. Initialize page 0 as leaf node
        void* rootNode = getPage(pager, 0);
        initializeLeafNode(rootNode);
        setNodeRoot(rootNode, true);
    }

    return table;
}

NodeType getNodeType(void* node) {
    uint8_t value = *((uint8_t*)((uint32_t*) node + NODE_TYPE_OFFSET));
    return (NodeType) value;
}

void setNodeType(void* node, NodeType type) {
    uint8_t value = type;
    *((uint8_t*)((uint32_t*)node + NODE_TYPE_OFFSET)) = value;
}

void splitLeafNodeAndInsert(Cursor* cursor, uint32_t key, Row* value) {
    // Create a new node and move half of cells over
    // Insert the new value in one of the two nodes
    // Update parent or create a new parent if needed
    void* oldNode = getPage(cursor->table->pager, cursor->page_num);
    uint32_t oldMax = getNodeMaxKey(oldNode);
    uint32_t newPageNum = getUnusedPageNum(cursor->table->pager);
    void* newNode = getPage(cursor->table->pager, newPageNum);
    initializeLeafNode(newNode);
    *nodeParent(newNode) = *nodeParent(oldNode);
    *leafNodeNextLeaf(newNode) =*leafNodeNextLeaf(oldNode);
    *leafNodeNextLeaf(oldNode) = newPageNum;

    // Now all existing keys plus the new key should be divided
    // evenly between old (left) and new (right) nodes.
    // Starting from the right, move each key to correct position
    for(int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--) {
        void* destinationNode;

        if (i >= LEAF_NODE_LEFT_SPLIT_COUNT) {
            destinationNode = newNode;
        } else {
            destinationNode = oldNode;
        }

        uint32_t indexWithinNode = i % LEAF_NODE_LEFT_SPLIT_COUNT;
        void* destination = leafNodeCell(destinationNode, indexWithinNode);
        if (i == cursor->cell_num) {
            // serializeRow(value, destination);
            serializeRow(value, leafNodeValue(destinationNode, indexWithinNode));
            *leafNodeKey(destinationNode, indexWithinNode) = key;
        } else if (i > cursor->cell_num) {
            memcpy(destination, leafNodeCell(oldNode, i - 1), LEAF_NODE_CELL_SIZE);
        } else {
            memcpy(destination, leafNodeCell(oldNode, i), LEAF_NODE_CELL_SIZE);
        }
    }

    *(leafNodeNumCells(oldNode)) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *(leafNodeNumCells(newNode)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

    // Now update the nodes' parent
    if (isRootNode(oldNode)) {
        return createNewRoot(cursor->table, newPageNum);
    } else {
        uint32_t parentPageNum = *nodeParent(oldNode);
        uint32_t newMax = getNodeMaxKey(oldNode);
        void* parent = getPage(cursor->table->pager, parentPageNum);
        updateInternalNodeKey(parent, oldMax, newMax);
        insertInternalNode(cursor->table, parentPageNum, newPageNum);
        return;
    }
}

void createNewRoot(Table* table, uint32_t rightChildPageNum) {
    // Old root is copied to new page and becomes left child
    // Address of right child is passed in
    // Re-initialize root page to contain the new root node
    // New root node points to two children

    void* root = getPage(table->pager, table->root_page_num);
    void* rightChild = getPage(table->pager, rightChildPageNum);
    uint32_t leftChildPageNum = getUnusedPageNum(table->pager);
    void* leftChild = getPage(table->pager, leftChildPageNum);

    // Left child has data copied from old root
    memcpy(leftChild, root, PAGE_SIZE);
    setNodeRoot(leftChild, false);

    initializeInternalNode(root);
    setNodeRoot(root, true);
    *internalNodeNumKeys(root) = 1;
    *internalNodeChild(root, 0) = leftChildPageNum;
    uint32_t leftChildMaxKey = getNodeMaxKey(leftChild);
    *internalNodeKey(root, 0) = leftChildMaxKey;
    *internalNodeRightChild(root) = rightChildPageNum;

    // In order to get a reference to parent node, we need to record in each node a pointer to parent
    *nodeParent(leftChild) = table->root_page_num;
    *nodeParent(rightChild) = table->root_page_num;

}

uint32_t* internalNodeNumKeys(void* node) {
    return (uint32_t*) node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

uint32_t* internalNodeRightChild(void* node) {
    return (uint32_t*) node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}

uint32_t* internalNodeCell(void* node, uint32_t cellNum) {
    return (uint32_t*) node + INTERNAL_NODE_HEADER_SIZE + cellNum * INTERNAL_NODE_CELL_SIZE;
}

uint32_t* internalNodeChild(void* node, uint32_t childNum) {
    uint32_t numKeys = *internalNodeNumKeys(node);
    if (childNum > numKeys) {
        printf("Tried to access child_num %d > num_keys %d\n", childNum, numKeys);
        exit(EXIT_FAILURE);
    } else if (childNum == numKeys) {
        return internalNodeRightChild(node);
    } else {
        return internalNodeCell(node, childNum);
    }
}

uint32_t* internalNodeKey(void* node, uint32_t keyNum) {
    return internalNodeCell(node, keyNum) + INTERNAL_NODE_CHILD_SIZE;
}

void updateInternalNodeKey(void* node, uint32_t oldKey, uint32_t newKey) {
    uint32_t oldChildIndex = internalNodeFindChild(node, oldKey);
    *internalNodeKey(node, oldChildIndex) = newKey;
}

// For an internal node, the max key is its right key
// For a leaf node, however, it's the key at the max index
uint32_t getNodeMaxKey(void* node) {
    switch(getNodeType(node)) {
        case NODE_INTERNAL:
            return *internalNodeKey(node, *internalNodeNumKeys(node) - 1);
        case NODE_LEAF:
            return *leafNodeKey(node, *leafNodeNumCells(node) - 1);
    }
}

// Getter and Setter functions to help keep track of the root node
bool isRootNode(void* node){
    uint8_t value = *((uint8_t*)((uint32_t*)node + IS_ROOT_OFFSET));
    return (bool) value;
}

void setNodeRoot(void* node, bool isRoot) {
    uint8_t value = isRoot;
    *((uint8_t*)((uint32_t*)node + IS_ROOT_OFFSET)) = value;
}



void initializeInternalNode(void* node) {
    setNodeType(node, NODE_INTERNAL);
    setNodeRoot(node, false);
    *internalNodeNumKeys(node) = 0;
}

// Meta Commands
void printConstants() {
    printf("ROW_SIZE: %d\n", ROW_SIZE);
    printf("COMMON_NODE_METADATA_SIZE: %d\n", COMMON_NODE_METADATA_SIZE);
    printf("LEAF_NODE_METADATA_SIZE: %d\n", LEAF_NODE_METADATA_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %ld\n", LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
    printf("LEAF_NODE_MAX_CELLS: %ld\n", LEAF_NODE_MAX_CELLS);
}

// Metadata functions to visualize the B-Tree
void indent(uint32_t level) {
    for(uint32_t i = 0; i < level; i++) {
        printf(" ");
    }
}

void print_tree(Pager* pager, uint32_t pageNum, uint32_t indentationLevel) {
    void* node = getPage(pager, pageNum);
    uint32_t numKeys, child;

    switch(getNodeType(node)) {
        case (NODE_LEAF):
            numKeys = *leafNodeNumCells(node);
            indent(indentationLevel);
            printf("- leaf (size %d)\n", numKeys);

            for (uint32_t i = 0; i < numKeys; i++) {
                indent(indentationLevel + 1);
                printf("- %d\n", *leafNodeKey(node, i));
            }
            break;
        case (NODE_INTERNAL):
            numKeys = *internalNodeNumKeys(node);
            indent(indentationLevel);
            printf("- internal (size %d)\n", numKeys);
            for (uint32_t i = 0; i < numKeys; i++) {
                child = *internalNodeChild(node, i);
                print_tree(pager, child, indentationLevel + 1);
                indent(indentationLevel + 1);
                printf("- key %d\n", *internalNodeKey(node, i));
            }

            child = *internalNodeRightChild(node);
            print_tree(pager, child, indentationLevel + 1);
            break;
    }
}

// Return the index of the child node which 'should' contain the given key value
uint32_t internalNodeFindChild(void* node, uint32_t key) {
    uint32_t numKeys = *internalNodeNumKeys(node);

    // Binary search to find index of child to search
    uint32_t minIndex = 0;
    uint32_t maxIndex = numKeys;

    while (minIndex != maxIndex) {
        uint32_t index = (minIndex + maxIndex) / 2;
        uint32_t keyToRight = *internalNodeKey(node, index);

        if (keyToRight >= key) {
            maxIndex = index;
        } else {
            minIndex = index + 1;
        }
    }

    return minIndex;
}

Cursor* internalNodeFind(Table* table, uint32_t pageNum, uint32_t key) {
    void* node = getPage(table->pager, pageNum);
    uint32_t numKeys = *internalNodeNumKeys(node);

    uint32_t childIndex = internalNodeFindChild(node, key);
    uint32_t childNum = *internalNodeChild(node, childIndex);
    void* child = getPage(table->pager, childNum);
    switch (getNodeType(child)) {
        case NODE_LEAF:
            return findLeafNode(table, childNum, key);
        case NODE_INTERNAL:
            return internalNodeFind(table, childNum, key);
    }
}

void insertInternalNode(Table* table, uint32_t parentPageNum, uint32_t childPageNum) {
    // Add a new child / key pair (aka cell) to corresponding parent node
    void* parent = getPage(table->pager, parentPageNum);
    void* child = getPage(table->pager, childPageNum);

    // The index where the new cell should be depends on the max key in the new child
    // If there's no room in the internal node for another cell, throw error (need to split internal node)
    uint32_t childMaxKey = getNodeMaxKey(child);
    uint32_t index = internalNodeFindChild(parent, childMaxKey);
    uint32_t originalNumKeys = *internalNodeNumKeys(parent);
    *internalNodeNumKeys(parent) = originalNumKeys + 1;

    if (originalNumKeys >= INTERNAL_NODE_MAX_CELLS) {
        printf("Need to split internal nodes\n");
        exit(EXIT_FAILURE);
    }

    uint32_t rightChildPageNum = *internalNodeRightChild(parent);
    void* rightChild = getPage(table->pager, rightChildPageNum);

    if (childMaxKey > getNodeMaxKey(rightChild)) {
        // Replace the right child
        *internalNodeChild(parent, originalNumKeys) = rightChildPageNum;
        *internalNodeKey(parent, originalNumKeys) = getNodeMaxKey(rightChild);
        *internalNodeRightChild(parent) = childPageNum;
    } else {
        // Make room for a new cell
        for (uint32_t i = originalNumKeys; i > index; i--) {
            void* destination = internalNodeCell(parent, i);
            void* source = internalNodeCell(parent, i - 1);
            memcpy(destination, source, INTERNAL_NODE_CELL_SIZE);
        }

        *internalNodeChild(parent, index) = childPageNum;
        *internalNodeKey(parent, index) = childMaxKey;
    }

}

int main(int argc, char* argv[]) {

    if (argc < 2) {
        printf("Must supply a databse filename\n");
        exit(EXIT_FAILURE);
    }

    char* filename = argv[1];
    Table* table = dbOpen(filename);

    InputBuffer* buffer = newInputBuffer();

    while (true) {
        printPrompt();
        readInput(buffer);

        if (buffer->buffer[0] == '.') {
            switch (execMetaCommand(buffer, table)) {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized command %s\n", buffer->buffer);
                    continue;
            }
        }
    
        
        Statement statement;
        switch (prepareStatement(buffer, &statement)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_NEGATIVE_ID):
                printf("ID must be a positive number\n");
                break;
            case (PREPARE_STRING_TOO_LONG):
                printf("String is too long\n");
                continue;
            case (PREPARE_SYNTAX_ERROR):
                printf("Syntax error. Could not parse statement\n");
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of '%s'\n", buffer->buffer);
                continue;
        }

        switch (executeStatement(&statement, table)) {
            case (EXECUTE_SUCCESS):
                printf("Executed\n");
                break;
            case (EXECUTE_TABLE_FULL):
                printf("Error: Table full\n");
                break;
        }
    }
}

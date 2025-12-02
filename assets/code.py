import os, struct

PAGE_SIZE = 4096  # Each page is 4 KB

def create_heap_file(file_name):
    with open(file_name, 'wb') as f:  # Open in write-binary mode
        pass
    
def read_page(file_name, page_number):
    """Read a specific page (4 KB) from the heap file given the page number."""
    # Check if the file has enough pages to contain the requested page
    file_size = os.path.getsize(file_name)
    last_page_number  = file_size // PAGE_SIZE -1

    if page_number > last_page_number:
        raise ValueError(f"Page {page_number} does not exist in the file.")

    with open(file_name, 'rb') as f:
        f.seek(page_number * PAGE_SIZE)  # Go to the start of the specified page
        page_data = f.read(PAGE_SIZE)  # Read the 4 KB page
    return page_data

def append_page(file_name, page_data):
    """Appends the provided page data to the end of the file."""
    if len(page_data) > PAGE_SIZE:
        raise ValueError(f"Page data must be exactly {PAGE_SIZE} bytes.")

    with open(file_name, 'ab') as f:  # Open in append-binary mode
        f.write(page_data)  # Append the page data to the end of the file
        
def write_page(file_name, page_number, page_data):
    """Write data to a specific page in the heap file."""
    # Check if the file has enough pages to contain the requested page
    file_size = os.path.getsize(file_name)
    last_page_number  = file_size // PAGE_SIZE -1

    if page_number > last_page_number:
        raise ValueError(f"Page {page_number} does not exist in the file.")
    if len(page_data) > PAGE_SIZE:
        raise ValueError(f"Data must be exactly {PAGE_SIZE} bytes long.")

    with open(file_name, 'r+b') as f:  # Open for reading and writing in binary mode
        f.seek(page_number * PAGE_SIZE)  # Go to the start of the specified page
        f.write(page_data)  # Write the data to the page

def Calculate_free_space(page_data):
  num_records= int.from_bytes(page_data[4094:4096],byteorder = 'big')
  records_size= int.from_bytes(page_data[4092:4094],byteorder = 'big')
  used_space = num_records * records_size
  free_space = len(page_data)-used_space-8
  return free_space

def insert_record_data_to_page_data(page_data, record_data):
    page_data = bytearray(page_data)
  # check the free space vs record length
    free_space_offset = int.from_bytes(page_data[4094:4096], byteorder='little')
    slot_count = int.from_bytes(page_data[4092:4094], byteorder='little')
    total_space = len(page_data)

    record_length = len(record_data)
    free_space = free_space_offset - (4 + (slot_count * 4))

    if record_length > free_space:
        raise ValueError("Insufficient space to insert the record.")
  # get the free space offset and slot count
    free_space_offset -= record_length
    new_slot_offset = free_space_offset
  # insert record_data starting from free space offset
    page_data[new_slot_offset:new_slot_offset + record_length] = record_data
  # inserting in the new slot entry (offset and length) of the new inserted record
    slot_entry_offset = 4 + (slot_count * 4)
    page_data[slot_entry_offset:slot_entry_offset + 2] = new_slot_offset.to_bytes(2, byteorder='little')
    page_data[slot_entry_offset + 2:slot_entry_offset + 4] = record_length.to_bytes(2, byteorder='little')
  # updating slot count and free space offset
    slot_count += 1
    page_data[0:2] = free_space_offset.to_bytes(2, byteorder='little')
    page_data[2:4] = slot_count.to_bytes(2, byteorder='little')
  # returns he page_data with inserted record in bytes format
    return bytes(page_data)

def insert_record_to_file(file_name, record_data):
  # Get the page that have enough free space

    try:
        with open(file_name, "rb") as file:
            pages = []
            while True:
                page = file.read(PAGE_SIZE)
                if not page:
                    break
                pages.append(bytearray(page))
    except FileNotFoundError:
        pages = []
  # If there is no free free space or the file is empty, create new data page of 4096 bytes
    record_inserted = False
    for i, page_data in enumerate(pages):
        free_space_offset = int.from_bytes(page_data[0:2], byteorder='little')
        slot_count = int.from_bytes(page_data[2:4], byteorder='little')
        free_space = free_space_offset - (4 + (slot_count * 4))

  # call insert_record_data_to_page_data function
        if len(record_data) <= free_space:
            pages[i] = insert_record_data_to_page_data(page_data, record_data)
            record_inserted = True
            break
    if not record_inserted:
        new_page = bytearray(b'\x00' * PAGE_SIZE)
        new_page[0:2] = PAGE_SIZE.to_bytes(2, byteorder='little')
        new_page[2:4] = (0).to_bytes(2, byteorder='little')
        new_page = insert_record_data_to_page_data(new_page, record_data)
        pages.append(new_page)

  # write or append the page
    with open(file_name, "wb") as file:
        for page in pages:
            file.write(page)
    print("Record successfully inserted.")
    
def get_record_from_page(page_data, record_id):
  # Retrieve a record from the specified page_data given the record ID.
    page_data = bytearray(page_data)

    slot_count = int.from_bytes(page_data[2:4], byteorder='little')

    if record_id < 0 or record_id >= slot_count:
        raise ValueError("Invalid record_id. It is out of range.")

    slot_offset = 4 + (record_id * 4)
    record_offset = int.from_bytes(page_data[slot_offset:slot_offset + 2], byteorder='little')
    record_length = int.from_bytes(page_data[slot_offset + 2:slot_offset + 4], byteorder='little')

    record_data = page_data[record_offset:record_offset + record_length]
    return bytes(record_data)

def get_record_from_file(file_name, page_number, record_id):
  #Retrieve a record from the specified page of the heap file given the record ID.
    try:
        with open(file_name, "rb") as file:

            file.seek(page_number * PAGE_SIZE)

            page_data = file.read(PAGE_SIZE)
            if not page_data:
                raise ValueError(f"Page {page_number} does not exist in the file.")

            record_data = get_record_from_page(page_data, record_id)
            return record_data
    except FileNotFoundError:
        raise FileNotFoundError(f"The file '{file_name}' does not exist.")
    


def get_all_record_from_page(page_data):

    page_data = bytearray(page_data)

    slot_count = int.from_bytes(page_data[2:4], byteorder='little')

    records = []

    for record_id in range(slot_count):

        slot_offset = 4 + (record_id * 4)
        record_offset = int.from_bytes(page_data[slot_offset:slot_offset + 2], byteorder='little')
        record_length = int.from_bytes(page_data[slot_offset + 2:slot_offset + 4], byteorder='little')

        record_data = page_data[record_offset:record_offset + record_length]
        records.append(bytes(record_data))

    return records




def get_all_record_from_file(file_name):
    # PAGE_SIZE = 4096

    try:
        with open(file_name, "rb") as file:
            all_records = []

            while True:
                page_data = file.read(PAGE_SIZE)
                if not page_data:
                    break

                records = get_all_record_from_page(page_data)
                # all_records.extend(records)
                all_records.append(records)

            return all_records
    except FileNotFoundError:
        raise FileNotFoundError(f"The file '{file_name}' does not exist.")
    
    
    
    file_name = 'file_name.bin'
    create_heap_file(file_name)
    page_data = b'new page data'
    page_data1 = b'new page data11111111111'
    append_page(file_name, page_data)
    append_page(file_name, page_data1)
    read_page(file_name,0)
    insert_record_to_file(file_name, b'BBBBBB')
    get_record_from_file(file_name, 0, 0)
    
    
  
from csv import reader
import csv
from etl import shared_funcs
import queue
import concurrent.futures
import os

OUTPUT_FILE_PATH = 'wh/fact1_c.csv'

def setup_output(fpath, headers):
    with open(fpath, mode= 'w') as file:
        file.write(','.join(headers)) # no commas allowed, would've had to do f'"{header}"'
        file.write(os.linesep)

def batch_to_csv(path, batch):
    # str = [os.linesep.join([','.join(batch.get()) for _ in range(batch.qsize())])]

    # lines = list(batch.get())
    lines = []
    for _ in range(batch.qsize()):
        row_data = tuple(batch.get())
        # print(row_data)
        # lines.append(','.join(str(x) for x in row_data))
        lines.append(row_data)

    out = os.linesep.join(str(lines))

    with open(path, mode='a') as file:
        file.write(out)
    pass


def batch_process_row(row: list, batch: queue.Queue, func, *args):
    # process and save in memory, then save to file and empty

    # this part can maybe also be batch threaded
    # processed = func(row, args)
    batch.put(row)

    if batch.full():
        batch_to_csv(OUTPUT_FILE_PATH, batch)

    return batch

# TEMPORARILY here while I figure things out
def create_dims(row, fact_list):
    time_id = shared_funcs.handle_time(row)
    # time_id = 0 
    # cardholder_id = 0
    # merchant_id = 0
    cardholder_id = shared_funcs.handle_loc(row, 2, 3)
    merchant_id = shared_funcs.handle_loc(row, 4, 5)
    if cardholder_id == -1 or merchant_id == -1:
        raise
    # cat1_id, cat2_id, cat3_id = (0,0,0) 
    # cat1_id, cat2_id, cat3_id = handle_categories(row)
    cat1_id = shared_funcs.handle_one_dim('category', row, 9)
    cat2_id = shared_funcs.handle_one_dim('category', row, 10)
    cat3_id = shared_funcs.handle_one_dim('category', row, 11)

    pan_cnt = row[6]
    txn_cnt = row[7]
    txn_gbp_amt = row[8]
    return (pan_cnt, txn_cnt, txn_gbp_amt, time_id, cardholder_id, merchant_id, cat1_id, cat2_id, cat3_id)




def etl(fpath):
    shared_funcs.ensure_dim('time', ['raw', 'year', 'quarter', 'month', 'id'])
    shared_funcs.ensure_dim('location',['sector', 'district', 'area', 'region', 'id'])
    shared_funcs.ensure_dim('category', ['category', 'id'])
    # gonna move the above header specs into shared_funcs and just do ensure_dims(names)

    fact_headers = ['pan_cnt', 'txn_cnt', 'txn_gbp_amt', 'time_id', 'cardholder_id', 'merchant_id', 'cat1_id', 'cat2_id', 'cat3_id']

    setup_output(OUTPUT_FILE_PATH, fact_headers)

    batch = queue.Queue(5000)
    # in_batch = queue.Queue(500) # for processing
    # out_batch = queue.Queue(500) # for writing

    print(1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        todo = []

        print(2)
        with open(fpath) as file:
            next(file) # skip headers
            csvreader = csv.reader(file)
            # print(3)
            for row in csvreader:
                # i dont really understand things
                future = executor.submit(batch_process_row, row, batch, create_dims)
                todo.append(future)
                # print(4)
            for future in concurrent.futures.as_completed(todo):
                # print(5)
                result = future.result()

            if not result.empty():
                print(result)


# spends about 4 seconds deadlocked somewhere somehow but only when there's more than 10k rows

# prob best to skip threading for now and only do batch stream loading

if __name__ == '__main__':
    etl('data/file1_1e5.csv')
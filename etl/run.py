import file1, file2
import distances_to_json, census_to_json
import time


def time_step(time_var, step_name):
    time1 = time.perf_counter()
    if time_var:
        print(f'Took {time1 - time_var} seconds to {step_name}.')
    return time1


if __name__ == '__main__':
    time0 = time_step(None, '')

    # TODO only do this if changed - either manually or with some hash like md5
    # distances_to_json.etl('generated_data/out_card_trips_car.csv')
    time0 = time_step(time0, 'convert distances csv into json')

    # census_to_json.etl_default_files()
    time0 = time_step(time0, 'create census json')

    # file1.etl_sample_file()
    # file1.etl_real_files()

    # file2.etl_sample_file()
    file2.etl_real_files()

    # file1.etl(['data/module1_sample_10k.zip'])
    # file1.etl('/mnt/sftp/module 1/san-ssapfs/edge/home/chaudhup/Network_Rail/nr_module_1_2021.csv')
    time0 = time_step(time0, 'load file1 into database')

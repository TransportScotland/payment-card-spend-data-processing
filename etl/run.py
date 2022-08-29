import file1, file2, file3, file4
import distances_to_json, census_to_json
import time


def time_step(time_var, step_name):
    """
    Print the time it took since the last call to this function.
    Usage example: time_tracker = None; time_tracker = time_step(time_tracker, 'step')    
    """
    time1 = time.perf_counter()
    if time_var:
        print(f'Took {time1 - time_var} seconds to {step_name}.')
    return time1


if __name__ == '__main__':
    time0 = time_step(None, '')

    # # convert the distances csv into json (only needed if the csv has been updated or the json is gone)
    # distances_to_json.etl('generated_data/out_card_trips_car.csv')
    # time0 = time_step(time0, 'convert distances csv into json')

    # # combine census CSVs with location data and save as json (only needed if CSVs updates or json gone)
    # census_to_json.etl_default_files()
    # time0 = time_step(time0, 'create census json')

    # # Load module 1 into the database
    # file1.etl_sample_file()
    # file1.etl_real_files()
    # time0 = time_step(time0, 'load module 1 into database')

    # # Load module 2 into the database
    # file2.etl_sample_file()
    # file2.etl_real_files()
    # time0 = time_step(time0, 'load module 2 into database')

    # # module 3
    # file3.etl_sample_file()
    file3.etl_real_files()
    time0 = time_step(time0, 'load module 3')


    # # module 4
    # file4.etl_sample_file()
    # file4.etl_real_files()
    # time0 = time_step(time0, 'load module 4')
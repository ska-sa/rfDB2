import numpy as np
import time

def metadata_extraction_test():
    import dbControl
    test_data = np.random.normal(size = 32768)
    db = dbControl.dbControl()
    start = time.time()
    metadata = db.calcMetaData(test_data)
    print ("metadata took %s seconds"%str(time.time() - start))
    print metadata

def insertDumpTest():
    import dbControl
    import monitor_conf as cnf
    import os
    import h5py

    print "Generating Data"
    test_data = np.random.normal(size = 32768)
    args = cnf.monitor_db

    print "Opening DB"
    db = dbControl.dbControl( args[0], args[1], args[2], args[3] )

    print "Creating file"
    timestamp = time.time()

    localtime = time.localtime(timestamp)
    fileName = "%02i.h5"%(localtime[3]) #Filename is the hour of the observation
    path = os.path.join('%s/rfi_data'%cnf.root_dir, str(localtime[0]), "%02i"%localtime[1], "%02i"%localtime[2],'') #Filepath year/month/day of the observation
    location = "%s%s"%(path,fileName)
    if not os.path.exists(path):
        os.makedirs(path)
    f = h5py.File(location, 'w')
    f.create_dataset('spectra', (3600, cnf.modes[0]['n_chan']),chunks = (4, cnf.modes[0]['n_chan']), dtype = type(test_data[0]), compression='gzip', compression_opts=4)

    print "Making environmental data"
    env_data={}
    env_data['adc_overrange'] = 0
    env_data['fft_overrange'] = 0
    env_data['adc_level'] = -25.89
    env_data['ambient_temp'] = 300
    env_data['adc_temp'] = 280
    env_data['mode'] = 1

    print "Inserting dump"
    db.insertDump(test_data, int(timestamp), env_data, f)

    print "closing connections and files"

    f.close()
    db.close()

def test_weave():
    import rfi_event
    data = np.random.normal(size=10)
    rfi_event.weave_median_filter(data, 10, 10, 10)

if __name__=="__main__":
    
    insertDumpTest()
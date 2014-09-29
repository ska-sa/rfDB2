import numpy as np
from scipy import weave
  
def get_rfi(data,sigma=4):
    """ Get the rfi for the middle of the window 
    Data is assumed to be a 2D array
    and sigma is the standard deviation that is used"""
    mid = data.shape[0]//2+1   # mid point of window
    if ( data.ndim == 2 ):
        med = np.median(data[:,:])
    else:
        med = np.median(data)

    mad = np.median(np.abs(data - med),axis=0)

    mad_limit = sigma/1.4826 # see relation to standard deviation
    return (data[mid]  > mad_limit*mad + med ) + (data[mid]  < -mad_limit*mad + med ) , mad_limit

def TD_median_filter (data, window=10, sigma = 4):
    rfi = np.zeros(data.shape, dtype=np.uint8)
    for t in range(window,data.shape[0]-window) :  rfi[t,:] = get_rfi(data[t-window:t+window+1])
    rfi = rfi[window/2:len(rfi) - window/2]
    # if ( data.shape[1] % 8 == 0 ):
    #     return np.packbits(rfi, axis = 1)
    # else:
    return rfi

def FD_median_filter (f_data, window=10, sigma = 6):
    import time
    start = time.time()
    data = np.transpose(f_data)
    print "transpose took %is"%int(time.time() - start)
    rfi = np.zeros(data.shape, dtype=np.uint8)
    rfi_threshold = np.zeros(data.shape, dtype = np.float32)
    if ( data.ndim == 2):
        for t in range(window,data.shape[0]-window) :  rfi[t,:], rfi_threshold[t,:] = get_rfi(data[t-window:t+window+1], sigma = sigma)
    else:
        import time
        start = time.time()
        for t in range(window,data.shape[0]-window) :  rfi[t] = get_rfi(data[t-window:t+window+1], sigma = sigma)
        print ("ave time = %f"%((time.time() -start)/(data.shape[0] - 2 * window)))
    print len(data)
    rfi = rfi[window:len(rfi) - window]
    print len(rfi)
    # if ( rfi.shape[0] % 8 == 0 ):
    #     return np.packbits(np.transpose(rfi), axis = rfi.ndim -1)
    # else:
    return np.transpose(rfi), np.transpose (rfi_threshold)

if __name__ == "__main__":
    import time
    import h5py
    f = h5py.File('/home/ratty2/monitor/13.h5', mode='r+')
    start = time.time()
    # indices=np.where(f['mode'][:] != 0)
    # print indices
    # td_mask = TD_median_filter(f['spectra'][:][indices], window = 20, sigma = 2)
    # td_mask = TD_median_filter(f['spectra'][:], window = 40, sigma = 6)
    print "TD_took %is"%int(time.time()-start)
    start = time.time()
    # fd_mask = FD_median_filter(f['spectra'][:][indices], window = 20, sigma = 10)
    fd_mask, fd_threshold = FD_median_filter(f['spectra'], window = 20, sigma = 6)
    print "FD_took %is"%int(time.time()-start)

    # fd_masked = np.ma.array(f['spectra'], mask = fd_mask)

    # td_masked = np.ma.array(f['spectra'], mask = td_mask)

    # import matplotlib.pyplot as plt
    # import matplotlib.cm as cm

    # fig = plt.figure()
    # one = fig.add_subplot(1,2,1)
    # # one.imshow(f['spectra'][:][indices], interpolation = 'none', cmap = cm.Blues)
    # one.imshow(f['spectra'][:], interpolation = 'none', cmap = cm.Blues)
    # one.set_aspect(10)
    # # plt.show()
    # one.set_title("Raw Data")
    # two = fig.add_subplot(1,2,2)
    # two.imshow(td_mask, interpolation = 'none', cmap = cm.Blues)
    # two.set_title("td mask")
    # two.set_aspect(10)
    # plt.show()
    # fig = plt.figure()
    # one = fig.add_subplot(1,2,1)
    # # one.imshow(f['spectra'][:][indices], interpolation = 'none', cmap = cm.Blues)
    # one.imshow(f['spectra'][:], interpolation = 'none', cmap = cm.Blues)
    # one.set_aspect(10)
    # three = fig.add_subplot(1,2,2)
    # three.imshow(fd_mask, interpolation = 'none', cmap = cm.Blues)
    # three.set_title("fd mask")
    # three.set_aspect(10)
    # # four = fig.add_subplot(2,2,4)
    # # four.imshow(fd_masked, interpolation = 'none', cmap = cm.Blues)
    # # four.set_title("fd masked")

    # plt.show()

    # andarr = np.logical_and(td_mask[:,40:-40],fd_mask[20:-20])

    # fig = plt.figure()
    # one = fig.add_subplot(1,2,1)
    # # one.imshow(f['spectra'][:][indices], interpolation = 'none', cmap = cm.Blues)
    # one.imshow(andarr, interpolation = 'none', cmap = cm.Blues)
    # one.set_aspect(10)
    # # plt.show()
    # one.set_title("and mask")
    # two = fig.add_subplot(1,2,2)
    # two.imshow(td_mask, interpolation = 'none', cmap = cm.Blues)
    # two.set_title("td mask")
    # two.set_aspect(10)
    # plt.show()

    # intersect = np.logical_and(andarr, td_mask[:,40:-40])
    # numtd = np.count_nonzero(td_mask)
    # numint = np.count_nonzero(td_mask)
    numfd = np.count_nonzero(fd_mask)

    # print "Number in td = %i, number in numint = %i, so FD covers %f%% of TD"%(numtd,numint,float(numint)/numtd * 100)
    print "FD calculates that %f%% of data is rfi"%(float(numfd)/(3600*14200)*100)

    # plt.savefig('rfi.png')



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
    return (data[mid]  > mad_limit*mad + med ) + (data[mid]  < -mad_limit*mad + med )

def TD_median_filter (data, window=10, sigma = 4):
    rfi = np.zeros(data.shape, dtype=np.uint8)
    for t in range(window,data.shape[0]-window) :  rfi[t,:] = get_rfi(data[t-window:t+window+1])
    rfi = rfi[window/2:len(rfi) - window/2]
    if ( data.shape[1] % 8 == 0 ):
        return np.packbits(rfi, axis = 1)
    else:
        return rfi

def FD_median_filter (data, window=10, sigma = 6):
    data = np.transpose(data)
    rfi = np.zeros(data.shape, dtype=np.uint8)
    if ( data.ndim == 2):
        for t in range(window,data.shape[0]-window) :  rfi[t,:] = get_rfi(data[t-window:t+window+1], sigma = sigma)
    else:
        import time
        start = time.time()
        for t in range(window,data.shape[0]-window) :  rfi[t] = get_rfi(data[t-window:t+window+1], sigma = sigma)
        print ("ave time = %f"%((time.time() -start)/(data.shape[0] - 2 * window)))
    print len(data)
    rfi = rfi[window:len(rfi) - window]
    print len(rfi)
    if ( rfi.shape[0] % 8 == 0 ):
        return np.packbits(np.transpose(rfi), axis = rfi.ndim -1)
    else:
        return np.transpose(rfi)

def weave_median_filter (data, window, length,  sigma):
    code="""
    printf("length = %i\n", length);
    printf("window = %i\n", window);
    printf("array");

    for (int i = 0; i < length; i++){
        printf("%f ", array[i]);
    }
    """

    simplecode = """printf("hello");"""

    simpletest = weave.inline(simplecode,[])
    #test = weave.inline(code, ['data','window','length'])


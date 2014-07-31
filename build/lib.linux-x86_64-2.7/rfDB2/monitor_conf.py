import csv
import sys
import os
import json
import numpy
import base64

def make_mode_file(mode):
    freqs = [chan_to_freq(i,mode['base_freq'],mode['n_chan'],mode['bandwidth']) for i in range(mode['n_chan'])]
    array_to_csv_file(freqs, '%s/etc/ratty2/%s'%(root_dir,mode['fileName']))

def csv_to_list (fileName):
    """Open a csv file and put data in a list"""
    fIn = csv.reader(open(fileName,'rb'),delimiter=',')

    return[row for row in fIn]

def csv_to_array (fileName):
    from numpy import genfromtxt
    return genfromtxt(fileName, delimiter=',')


def array_to_csv_file (data, file_name, headings = ()):
    """save a list as a csv file"""
    import csv
    import numpy
    print headings

    f = open(file_name, 'w+')
    writer = csv.writer(f, delimiter = ',')

    if len(headings) >= 1:
        writer.writerow(headings)
    for row in data:
        if isinstance(row, list):
            writer.writerow(row)
        else:
            writer.writerow((row,))

    f.close()


"""Encode numpoy array as a base64 string"""
def Base64Encode(ndarray):
    return json.dumps([str(ndarray.dtype),base64.b64encode(ndarray),ndarray.shape])

"""Decode a base64 string to a numpy array"""
def Base64Decode(jsonDump):
    loaded = json.loads(jsonDump)
    dtype = numpy.dtype(loaded[0])
    arr = numpy.frombuffer(base64.decodestring(loaded[1]),dtype)
    if len(loaded) > 2:
        return arr.reshape(loaded[2])
    return arr

def freq_to_chan(base_freq,frequency,n_chans, bandwidth):
    """Returns the channel number where a given frequency is to be found. Frequency is in Hz."""
    #if frequency<0: 
    #    frequency=self.config['bandwidth']+frequency
    return round(float(frequency - base_freq)/bandwidth*n_chans)%n_chans

def chan_to_freq(chan,base_freq,n_chans, bandwidth):
    """Returns the channel number where a given frequency is to be found. Frequency is in Hz."""
    #if frequency<0: 
    #    frequency=self.config['bandwidth']+frequency
    return float(chan) * float(bandwidth)/float(n_chans) + base_freq

def getFreqs(mode):
    return csv_to_array("%s/etc/ratty2/%s"%(root_dir, modes[mode-1]['fileName']))

def low_chan (mode):
    low_freq = getFreqs(mode)[0]/10e5
    bandwidth = getFreqs(mode)[-1]/10e5 - getFreqs(mode)[0]/10e5
    print modes[mode - 1]['base_freq'] + modes[mode - 1]['bandwidth']
    print modes[mode-1]['base_freq']
    print chan_to_freq(freq_to_chan(low_freq, modes[mode - 1]['base_freq'], modes[mode-1]['n_chan'], bandwidth), low_freq, modes[mode-1]['n_chan'], bandwidth)
    return freq_to_chan(low_freq, modes[mode-1]['base_freq'], modes[mode-1]['n_chan'], bandwidth)

def high_chan (mode):
    low_freq = getFreqs(mode)[0] /10e5
    bandwidth = getFreqs(mode)[-1]/10e5 - getFreqs(mode)[0]/10e5
    print modes[mode - 1]['base_freq'] + modes[mode - 1]['bandwidth']
    print chan_to_freq(freq_to_chan(low_freq, modes[mode - 1]['base_freq'] + modes[mode - 1]['bandwidth'], modes[mode-1]['n_chan'], bandwidth), low_freq, modes[mode-1]['n_chan'], bandwidth)
    return freq_to_chan(low_freq, modes[mode - 1]['base_freq'] + modes[mode - 1]['bandwidth'], modes[mode-1]['n_chan'], bandwidth)




root_dir='/home/ratty2/monitor'
monitor_db=('localhost', 'root', 'kat', 'monitor')
curr=('localhost', 'root', 'kat', 'current_spectra')
modes=[{'id':1,
        'n_chan':32768,
        'bandwidth':650,
        'base_freq':100,
        'fileName':"mode1.csv",
        'low_chan':3641,
        'high_chan':27308},
        {'id':2,
        'n_chan':32768,
        'bandwidth':400,
        'base_freq':650,
        'fileName':"mode2.csv",
        'low_chan':2731,
        'high_chan':24577},
        {'id':3,
        'n_chan':32768,
        'bandwidth':770,
        'base_freq':900,
        'fileName':"mode3.csv",
        'low_chan':1725,
        'high_chan':31236},
        {'id':4,
        'n_chan':32768,
        'bandwidth':500,
        'base_freq':1950,
        'fileName':"mode4.csv",
        'low_chan':5462,
        'high_chan':23667}]
mode_chan_freq = []
for m in modes:
    if not os.path.isfile('%s/etc/ratty2/%s'%(root_dir,m['fileName'])):
        make_mode_file(m);
    mode_chan_freq.append(csv_to_list('%s/etc/ratty2/%s'%(root_dir,m['fileName'])))

print "Mode 1 low_chan, high_chan"
print high_chan(1)
print "Mode 2 low_chan, high_chan"
print low_chan (2)
print high_chan(2)
print "Mode 3 low_chan, high_chan"
print low_chan (3)
print high_chan(3)
print "Mode 4 low_chan, high_chan"
print low_chan (4)
print high_chan(4)
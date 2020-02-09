#-*- coding: UTF-8 -*-

from mpi4py import MPI 
import adios2
import numpy as np 


class reader_base():
    def __init__(self, shotnr, id, virtual_rank=None, channel=None):
        if virtual_rank is None:
            comm = MPI.COMM_WORLD
            self.rank = comm.Get_rank()
            self.size = comm.Get_size()
        else:
            self.rank = virtual_rank
            self.size = 1

        self.shotnr = shotnr
        self.id = id
        self.adios = adios2.ADIOS(MPI.COMM_SELF)
        self.channel = channel
        if self.channel is None:
            self.IO = self.adios.DeclareIO("stream_{0:03d}".format(self.rank))
        else:
            self.IO = self.adios.DeclareIO("stream_{0:03d}_s{1:03d}".format(self.rank, self.channel))
        self.reader = None

    def Open(self):
        """Opens a new channel"""
        if self.channel is None:
            self.channel_name = "{0:05d}_ch{1:06d}.bp".format(self.shotnr, self.id)
        else:
            self.channel_name = "{0:05d}_ch{1:06d}_s{2:03d}.bp".format(self.shotnr, self.id, self.channel)
        print (">>> Reader Opening ... %s"%(self.channel_name))

        if self.reader is None:
            self.reader = self.IO.Open(self.channel_name, adios2.Mode.Read)


    def BeginStep(self):
        """wrapper for reader.BeginStep()"""
        return self.reader.BeginStep()


    def InquireVariable(self, varname):
        """Wrapper for IO.InquireVariable"""
        return self.IO.InquireVariable(varname)


    def get_data(self, varname):
        """Attempt to load `varname` from the opened stream"""

        var = self.IO.InquireVariable(varname)
        io_array = np.zeros(np.prod(var.Shape()), dtype=np.float)
        self.reader.Get(var, io_array, adios2.Mode.Sync)

        return(io_array)


    def CurrentStep(self):
        """Wrapper for IO.CurrentStep()"""
        return self.reader.CurrentStep()

    
    def EndStep(self):
        """Wrapper for reader.EndStep()"""
        self.reader.EndStep()


class reader_dataman(reader_base):
    def __init__(self, shotnr, id):
        super().__init__(shotnr, id)
        self.IO.SetEngine("DataMan")
        self.reader = None

        dataman_port = 12300 + self.rank
        transport_params = {"IPAddress": "203.230.120.125",
                            "Port": "{0:5d}".format(dataman_port),
                            "OpenTimeoutSecs": "600",
                            "AlwaysProvideLatestTimestep": "true"}
        self.IO.SetParameters(transport_params)
        print (">>> reader_dataman ... ")


class reader_bpfile(reader_base):
    def __init__(self, shotnr, id):
        super().__init__(shotnr, id)
        self.IO.SetEngine("BP4")
        self.IO.SetParameter("OpenTimeoutSecs", "600")
        self.reader = None
        print (">>> reader_bpfile ... ")

class reader_sst(reader_base):
    def __init__(self, shotnr, id):
        super().__init__(shotnr, id)
        self.IO.SetEngine("SST")
        self.IO.SetParameters({"OpenTimeoutSecs": "600.0"})
        self.reader = None
        print (">>> reader_sst ... ")

class reader_gen(reader_base):
    """ General reader to be initialized by name and parameters
    """
    def __init__(self, shotnr, id, engine, params, virtual_rank=None, channel=None):
        super().__init__(shotnr, id, virtual_rank=virtual_rank, channel=channel)
        self.IO.SetEngine(engine)
        _params = params
        if engine.lower() == "dataman":
            if self.channel is None:
                dataman_port = 12300 + self.rank
            else:
                dataman_port = 12400 + 10*self.channel
            print (">>> Reader dataman_port: %d"%dataman_port)
            _params.update(Port = "{0:5d}".format(dataman_port))
        self.IO.SetParameters(_params)

# end of file readers.py

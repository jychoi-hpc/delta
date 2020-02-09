# -*- coding: UTF-8 -*-

from mpi4py import MPI
import adios2
import numpy as np

class writer_base():
    """
    params
    shotnr:
    id:
    virtual_rank: set rank with hard-coded value when no mpi available
    channel: channel id. It will be used to stream NERSC DTN to Cori computer nodes.
    """
    def __init__(self, shotnr, id, virtual_rank=None, channel=None):
        if virtual_rank is None:
            comm = MPI.COMM_WORLD
            self.rank = comm.Get_rank()
            self.size = comm.Get_size()
        else:
            self.rank = virtual_rank
            self.size = 1
        #print("writer_base.__init__(): rank = {0:02d}".format(self.rank))

        self.shotnr = shotnr
        self.id = id
        self.adios = adios2.ADIOS(MPI.COMM_SELF)
        self.channel = channel
        if self.channel is None:
            self.IO = self.adios.DeclareIO("stream_{0:03d}".format(self.rank))
        else:
            self.IO = self.adios.DeclareIO("stream_{0:03d}_s{1:03d}".format(self.rank, self.channel))
        self.writer = None


    def DefineVariable(self, data_array):
        """Wrapper around DefineVariable

        Input:
        ======
        data_array, ndarray: numpy array with sme number of elements and data type that will be sent in 
                             all subsequent steps
        """
        self.io_array = self.IO.DefineVariable("floats", data_array, 
                                               data_array.shape, 
                                               list(np.zeros_like(data_array.shape, dtype=int)), 
                                               data_array.shape, 
                                               adios2.ConstantDims)


    def Open(self):
        """Opens a new channel. 
        """

        if self.channel is None:
            self.channel_name = "{0:05d}_ch{1:06d}.bp".format(self.shotnr, self.id)
        else:
            self.channel_name = "{0:05d}_ch{1:06d}_s{2:03d}.bp".format(self.shotnr, self.id, self.channel)
        print (">>> Writer opening ... %s"%(self.channel_name))

        if self.writer is None:
            self.writer = self.IO.Open(self.channel_name, adios2.Mode.Write)


    def put_data(self, data):
        """Opens a new stream and send data through it
        Input:
        ======
        data: ndarray, float. Data to send)
        """

        if self.writer is not None:
            self.writer.BeginStep()
            self.writer.Put(self.io_array, data, adios2.Mode.Sync)
            self.writer.EndStep()


    def __del__(self):
        """Close the IO."""
        if self.writer is not None:
            self.writer.Close()



class writer_dataman(writer_base):
    def __init__(self, shotnr, id):
        super().__init__(shotnr, id)
        self.IO.SetEngine("DataMan")
        dataman_port = 12300 + self.rank
        transport_params = {"IPAddress": "203.230.120.125",
                            "Port": "{0:5d}".format(dataman_port),
                            "OpenTimeoutSecs": "600",
                            "Verbose": "20"}
        self.IO.SetParameters(transport_params)

class writer_bpfile(writer_base):
    def __init__(self, shotnr, id):
        super().__init__(shotnr, id)
        self.IO.SetEngine("BP4")
        
class writer_sst(writer_base):
    def __init__(self, shotnr, id):
        super().__init__(shotnr, id)
        self.IO.SetEngine("SST")
        self.IO.SetParameter("OpenTimeoutSecs", "600")

class writer_gen(writer_base):
    """ General writer to be initialized by name and parameters
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
            print (">>> Writer dataman_port: %d"%dataman_port)
            _params.update(Port = "{0:5d}".format(dataman_port))
        self.IO.SetParameters(_params)

# End of file a2_sender.py

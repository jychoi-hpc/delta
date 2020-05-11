#-*- Coding: UTF-8 -*-

from mpi4py import MPI
import adios2
import logging
import json
from os.path import join

import numpy as np

from analysis.channels import channel, channel_range
from streaming.adios_helpers import gen_io_name, gen_channel_name_v2

"""
Author: Ralph Kube
"""


class reader_base():
    def __init__(self, cfg: dict, shotnr: int=18431):
        """Generates a reader for KSTAR ECEI data.

        Parameters:
        -----------
        cfg: delta config dictionary
        """

        comm  = MPI.COMM_SELF
        self.rank = comm.Get_rank()
        self.size = comm.Get_size()
        # This should be MPI.COMM_SELF, not MPI.COMM_WORLD
        self.adios = adios2.ADIOS(MPI.COMM_SELF)
        self.logger = logging.getLogger("simple")

        self.shotnr = shotnr
        self.IO = self.adios.DeclareIO(gen_io_name(self.shotnr))
        # Keeps track of the past chunk sizes. This allows to construct a dummy time base
        self.chunk_sizes = []
        
        # Store configuration of the ECEI diagnostic
        self.ecei_cfg = cfg["ECEI_cfg"]

        # If false, indicates that raw data is returned.
        # If true, indicates that normalized data is returned.
        # This flag is set in Get()
        self.got_normalization = False

        # Defines the time where we take the offset
        self.tnorm = cfg["ECEI_cfg"]["t_norm"]

        self.reader = None
        # Generate a descriptive channel name
        self.chrg = channel_range.from_str(cfg["channel_range"][self.rank])
        self.channel_name = gen_channel_name_v2(self.shotnr, self.chrg.to_str())
        self.logger.info(f"reader_base: channel_name =  {self.channel_name}")


    def Open(self):
        """Opens a new channel"""

        self.logger.info(f"Waiting to receive channel name {self.channel_name}")
        if self.reader is None:
            self.reader = self.IO.Open(self.channel_name, adios2.Mode.Read)
        else:
            pass
        self.logger.info(f"Opened channel {self.channel_name}")

        return None

    def BeginStep(self):
        """Wrapper for reader.BeginStep()"""
        res = self.reader.BeginStep()
        if res == adios2.StepStatus.OK:
            return(True)
        return(False)


    def CurrentStep(self):
        """Wrapper for IO.CurrentStep()"""
        res = self.reader.CurrentStep()
        return(res)


    def EndStep(self):
        """Wrapper for reader.EndStep"""
        res = self.reader.EndStep()
        return(res)


    def InquireVariable(self, varname: str):
        """Wrapper for IO.InquireVariable"""
        res = self.IO.InquireVariable(varname)
        return(res)


    def get_attrs(self, attrsname: str):
        """Inquire json string `attrsname` from the opened stream"""

        attrs = self.IO.InquireAttribute(attrsname)
        return json.loads(attrs.DataString()[0])


    def gen_timebase(self):
        """Create a dummy time base for chunk last read."""

        # Unpack the trigger time, plasma time and end time from TriggerTime
        tt0, pl, tt1 = self.ecei_cfg["TriggerTime"]
        # Get the sampling frequency, in units of Hz
        fs = self.ecei_cfg["SampleRate"] * 1e3

        # The time base starts at t0. Assume samples are streaming in continuously with sampling frequency fs.
        # Offset of the first sample in current chunk
        offset = sum(self.chunk_sizes[:-1])
        t_sample = 1. / fs
        # Use integers in arange to avoid round-off errors. We want exactly chunk_sizes[-1] elements in
        # this array
        tb = np.arange(offset, offset + self.chunk_sizes[-1], dtype=np.float64)
        # Scale timebase with t_sample and shift to tt0
        tb = (tb * t_sample) + tt0        
        return(tb)

    
    def Get(self, ch_rg: channel_range, save: bool=False):
        """Get data from varname at current step. This is diagnostic-independent code.

        Inputs:
        =======
        ch_rg: channel_range that describes which channels to inquire. This is used to generate
               a variable name which is inquired from the stream

        Returns:
        ========
        time_chunk: numpy ndarray containing data of the current step
        """


        # elif isinstance(channels, type(None)):
        self.logger.info(f"Reading varname {ch_rg.to_str()}. Step no. {self.CurrentStep():d}")
        var = self.IO.InquireVariable(ch_rg.to_str())
        time_chunk = np.zeros(var.Shape(), dtype=np.float64)
        self.reader.Get(var, time_chunk, adios2.Mode.Sync)
        self.logger.info(f"Got data: {time_chunk.shape}, mean = {time_chunk.mean()}")
        # Append size of current chunk to chunk sizes
        self.chunk_sizes.append(time_chunk.shape[1])
        if save:
            np.savez(f"test_data/time_chunk_tr_s{self.CurrentStep():04d}.npz", time_chunk=time_chunk)

        return time_chunk


class reader_gen(reader_base):
    def __init__(self, cfg: dict, shotnr: int=18431):
        """Instantiates a reader.
           Control Adios method and params through cfg

        Parameters:
        -----------
        cfg : delta config dict
        """
        super().__init__(cfg, shotnr)
        self.IO.SetEngine(cfg["transport"]["engine"])
        self.IO.SetParameters(cfg["transport"]["params"])
        self.channel_name = gen_channel_name_v2(self.shotnr, self.chrg.to_str())
        self.reader = None

# End of file 
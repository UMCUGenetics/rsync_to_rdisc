"""Settings used in rsync_to_rdisc.py"""

""" General settings """
wkdir = "/diaggen/data/upload/"
temperror = "{}temp.error".format(wkdir)
log = "Rsync_Dx.log"
errorlog = "Rsync_Dx.errorlog"
bgarray = "/mnt/bgarray/"


""" Server/user settings """
host_keys = ""
server = ""
user = ""


"""Mail finished transfer"""
email_from = ""
email_to = ["", ""]


""" Transfer folders  """
# folder_dic: value items
# 1 = hpc location,
# 2 = transfer location,
# 3 = list of one or more files to be checked (note that all files need to be present),
# 4 = True/False boolean, in which True = continue without mail, and False = send error mail and stop
folder_dic = {
    "Exomes": {
        "input": "/hpc/diaggen/data/upload/Exomes/",
        "output": "{}Illumina/Exomes/".format(bgarray),
        "files_required": ["workflow.done"],
        "continue_without_email": False
    },
    "Genomes": {
        "input": "/hpc/diaggen/data/upload/Genomes/",
        "output": "{}Illumina/Genomes/".format(bgarray),
        "files_required": ["workflow.done"],
        "continue_without_email": False
    },
    "MIPS": {
        "input": "/hpc/diaggen/data/upload/MIPS/",
        "output": "{}Illumina/MIPS/".format(bgarray),
        "files_required": ["workflow.done"],
        "continue_without_email": False
    },
    "RAW_data_MIPS_nextseq_umc01": {
        "input": "/hpc/diaggen/data/upload/RAW_data_MIPS/nextseq_umc01/",
        "output": "{}RAW_data/MIPS/".format(bgarray),
        "files_required": ["TransferDone.txt"],
        "continue_without_email": True
    },
    "RAW_data_MIPS_nextseq_umc02": {
        "input": "/hpc/diaggen/data/upload/RAW_data_MIPS/nextseq_umc02/",
        "output": "{}RAW_data/MIPS/".format(bgarray),
        "files_required": ["TransferDone.txt"],
        "continue_without_email": True
    },
    "TRANSFER": {
        "input": "/hpc/diaggen/data/upload/TRANSFER/",
        "output": "{}TRANSFER/".format(bgarray),
        "files_required": [""],
        "continue_without_email": False
    },
    "RAW_data": {
        "input": "/hpc/diaggen/data/upload/RAW_data/",
        "output": "{}RAW_data/BACKUP_TEMP/".format(bgarray),
        "files_required": [""],
        "continue_without_email": False
    },
}

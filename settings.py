"""Settings used in rsync_to_rdisc.py"""

""" General settings """
wkdir = "/diaggen/data/upload/"
temperror = "{}temp.error".format(wkdir)
log = "Rsync_Dx.log"
errorlog = "Rsync_Dx.errorlog"
bgarray = "/mnt/bgarray/"


""" Server/user settings """
host_keys = ''
server = ''
user = ''


"""Mail finished transfer"""
email_from = ''
email_to = ['', '']


""" Transfer folders  """
# folder_dic: value items
# 1 = hpc location,
# 2 = transfer location,
# 3 = file to be checked,
# 4 = True/False, in which True = continue without mail, and False = send error mail and stop
folder_dic = {
    "Exomes": [
        "/hpc/diaggen/data/upload/Exomes/",
        "{}Illumina/Exomes/".format(bgarray),
        "workflow.done",
        "False"
    ],
    "Genomes": [
        "/hpc/diaggen/data/upload/Genomes/",
        "{}Illumina/Genomes/".format(bgarray),
        "workflow.done",
        "False"
    ],
    "MIPS": [
        "/hpc/diaggen/data/upload/MIPS/",
        "{}Illumina/MIPS/".format(bgarray),
        "workflow.done",
        "False"
    ],
    "RAW_data_MIPS_nextseq_umc01": [
        "/hpc/diaggen/data/upload/RAW_data_MIPS/nextseq_umc01/",
        "{}RAW_data/MIPS/".format(bgarray),
        "TransferDone.txt",
        "True"
    ],
    "RAW_data_MIPS_nextseq_umc02": [
        "/hpc/diaggen/data/upload/RAW_data_MIPS/nextseq_umc02/",
        "{}RAW_data/MIPS/".format(bgarray),
        "TransferDone.txt",
        "True"
    ],
    "TRANSFER": [
        "/hpc/diaggen/data/upload/TRANSFER/",
        "{}TRANSFER/".format(bgarray),
        "",
        "False"
    ],
    "RAW_data": [
        "/hpc/diaggen/data/upload/RAW_data/",
        "{}RAW_data/BACKUP_TEMP/".format(bgarray),
        "",
        "False"
    ],
}

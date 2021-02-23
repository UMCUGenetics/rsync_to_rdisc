"""Settings used in rsync_to_rdisc.py"""
wkdir = "/hpc/diaggen/data/upload/"
log = "Rsync_Dx.log"
errorlog = "Rsync_Dx.errorlog"
temperror = "/hpc/diaggen/data/upload/temp.error"
bgarray = "/data/diaggen/DIT-bgarray" 

folder_dic = {"Exomes":"/data/diaggen/DIT-bgarray/Illumina/Exomes", 
    "TRANSFER":"/data/diaggen/DIT-bgarray/TRANSFER", 
    "MIPS":"/data/diaggen/DIT-bgarray/Illumina/MIPS", 
    "RAW_data_MIPS":"/data/diaggen/DIT-bgarray/RAW_data/MIPS", 
    "RAW_data":"/data/diaggen/DIT-bgarray/RAW_data/BACKUP_TEMP", 
    "Genomes":"/data/diaggen/DIT-bgarray/Illumina/Genomes"}

"""Mail finished transfer"""
email_from = '<email>'

finished_mail = ['<email>']

owner_dic = {'<username1>':'<email1>','<username2>':'<email2>'}


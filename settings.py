"""Settings used in rsync_to_rdisc.py"""
wkdir="/hpc/diaggen/data/upload/"
log="Rsync_Dx.log"
errorlog="Rsync_Dx.errorlog"
temperror="/hpc/diaggen/data/upload/temp.error"

folder_dic = {"Exomes":"/data/DIT-bgarray/Illumina/Exomes","TRANSFER":"/data/DIT-bgarray/TRANSFER","MIPS":"/data/DIT-bgarray/Illumina/MIPS","RAW_data_MIPS":"/data/DIT-bgarray/RAW_data/MIPS","RAW_data":"/data/DIT-bgarray/RAW_data/BACKUP_TEMP","Genomes":"/data/DIT-bgarray/Illumina/Genomes"}

"""Mail finished transfer"""
email_from='<email>'

finished_mail=['<email>'
               ]

owner_dic={'<username1>':'<email1>',
           '<username2>':'<email2>',
          }


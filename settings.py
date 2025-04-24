"""Settings used in rsync_to_rdisc.py"""

""" General settings """
# Log files
wkdir = "/diaggen/data/upload/"
temp_error_path = f"{wkdir}/temp.error"
log_path = f"{wkdir}/Rsync_Dx.log"
errorlog_path = f"{wkdir}/Rsync_Dx.errorlog"

# Tools
alissa_vcf_upload = "/diaggen/software/production/alissa_vcf_upload/"

""" Server/user settings """
host_keys = ""
server = ["", ""]
user = ""

"""Mail finished transfer"""
email_smtp_host = "pim.umcutrecht.nl"
email_smtp_port = 25
email_from = ""
email_to = ["", ""]

""" Transfer settings  """
# transfer_settings: dict of dicts, where each dict is a mount point
# mount_path = path to mount point
# transfers: dict of dicts, where each dict is a folder to be transferred
#   input = hpc location,
#   output = transfer location,
#   files_required = list of one or more files to be checked (note that all files need to be present),
#   continue_without_email = True/False boolean, in which True = continue without mail, and False = send error mail and stop
#   upload_gatk_vcf = True/False, upload gatk vcf files, assumes vcf files in folder <run>/single_sample_vcf/
#   upload_exomedepth_vcf = True/False, upload exomedepth vcf files, assumes vcf files in folder <run>/exomedepth/HC/

transfer_settings = {
    "bgarray": {
        "mount_path": "/mnt/bgarray/",
        "transfers": [
            {
                "name": "Exomes",
                "input": "/hpc/diaggen/data/upload/Exomes/",
                "output": "Illumina/Exomes/",
                "files_required": ["workflow.done"],
                "continue_without_email": False,
                "upload_gatk_vcf": True,
                "upload_exomedepth_vcf": True,
            },
            {
                "name": "Genomes",
                "input": "/hpc/diaggen/data/upload/Genomes/",
                "output": "Illumina/Genomes/",
                "files_required": ["workflow.done"],
                "continue_without_email": False,
                "upload_gatk_vcf": False,
                "upload_exomedepth_vcf": False,
            },
            {
                "name": "TRANSFER",
                "input": "/hpc/diaggen/data/upload/TRANSFER/",
                "output": "TRANSFER/",
                "files_required": [""],
                "continue_without_email": False,
                "upload_gatk_vcf": False,
                "upload_exomedepth_vcf": False,
            },
            {
                "name": "RAW_data",
                "input": "/hpc/diaggen/data/upload/RAW_data/",
                "output": "RAW_data/BACKUP_TEMP/",
                "files_required": [""],
                "continue_without_email": False,
                "upload_gatk_vcf": False,
                "upload_exomedepth_vcf": False,
            },
            {
                "name": "Transcriptomes",
                "input": "/hpc/diaggen/data/upload/Transcriptomes/",
                "output": "Illumina/Transcriptomes",
                "files_required": ["workflow.done"],
                "continue_without_email": False,
                "upload_gatk_vcf": False,
                "upload_exomedepth_vcf": False,
            },
            {
                "name": "RAW_data_RNAseq",
                "input": "/hpc/diaggen/data/upload/RAW_data_RNAseq/",
                "output": "Validation/RAW_data/RNAseq/",
                "files_required": [""],
                "continue_without_email": False,
                "upload_gatk_vcf": False,
                "upload_exomedepth_vcf": False,
                "include": [
                    "**/",
                    "Data",
                    "Data/Intensities",
                    "Data/Intensities/BaseCalls",
                    "*RNASeq**",
                    "*Reports**",
                    "RunInfo.xml",
                    "RunParameters.xml",
                    "SampleSheet.csv",
                    "md5sum.txt",
                ],
                "exclude": ["*"],
            },
        ],
    },
    "glims": {
        "mount_path": "/mnt/glims/",
        "transfers": [
            {
                "name": "pg_glims",
                "input": "/hpc/diaggen/data/upload/pg_glims/",
                "output": "NGS_data",
                "files_required": [""],
                "continue_without_email": False,
                "upload_gatk_vcf": False,
                "upload_exomedepth_vcf": False,
            },
        ],
    },
}

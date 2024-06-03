import os

notebook_path = 'D:\\Users\\lilig\\Desktop\\github\\SMS_Report\\Daily_Reporting\\Data_Cleaning.ipynb'
notebook_path1 = 'D:\\Users\\lilig\\Desktop\\github\\SMS_Report\\Daily_Reporting\\SMS_Daily_Update.ipynb'
notebook_path2 = 'D:\\Users\\lilig\\Desktop\\github\\SMS_Report\\Daily_Reporting\\SMS_Offer_Alert_Report.ipynb'
notebook_path3 = 'D:\\Users\\lilig\\Desktop\\github\\SMS_Report\\Daily_Reporting\\Account_Feedback_Report.ipynb'
notebook_path4 = 'D:\\Users\\lilig\\Desktop\\github\\SMS_Report\\Daily_Reporting\\Upcoming_schedule_Swap_Report.ipynb'
notebook_path5 = 'D:\\Users\\lilig\\Desktop\\github\\SMS_Report\\Daily_Reporting\\Content_Feedback_Report_SMS.ipynb'
output_format = 'script'  # You can change this to other formats like 'pdf' or 'script'

# Build the command to run the notebook and convert it to the desired format
command = f'jupyter nbconvert --to {output_format} {notebook_path}'
command1 = f'jupyter nbconvert --to {output_format} {notebook_path1}'
command2 = f'jupyter nbconvert --to {output_format} {notebook_path2}'
command3 = f'jupyter nbconvert --to {output_format} {notebook_path3}'
command4 = f'jupyter nbconvert --to {output_format} {notebook_path4}'
command5 = f'jupyter nbconvert --to {output_format} {notebook_path5}'
# Execute the command
os.system(command)
os.system(command1)
os.system(command2)
os.system(command3)
os.system(command4)
os.system(command5)
#os.system('python D:\\Users\\lilig\\Desktop\\github\\SMS_Report\\Daily_Reporting\\Upcoming_schedule_Swap_Report.py')
os.system('python D:\\Users\\lilig\\Desktop\\github\\SMS_Report\\Daily_Reporting\\Data_Cleaning.py')
os.system('python D:\\Users\\lilig\\Desktop\\github\\SMS_Report\\Daily_Reporting\\SMS_Daily_Update.py')
os.system('python D:\\Users\\lilig\\Desktop\\github\\SMS_Report\\Daily_Reporting\\SMS_Offer_Alert_Report.py')
os.system('python D:\\Users\\lilig\\Desktop\\github\\SMS_Report\\Daily_Reporting\\Account_Feedback_Report.py')
os.system('python D:\\Users\\lilig\\Desktop\\github\\SMS_Report\\Daily_Reporting\\Content_Feedback_Report_SMS.py')


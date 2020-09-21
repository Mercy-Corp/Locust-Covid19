import os

def removefile(filepath):
  try:
    os.remove(filepath)
  except OSError:
    print('File ' + filepath + ' could not be deleted')

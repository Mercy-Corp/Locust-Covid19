import yaml

with open("config/application.yaml", "r") as ymlfile:
    cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)

for key in ('landing','reporting'):
	print('%s : %s, %s' % (key, cfg["data"][key], type(cfg["data"][key])))

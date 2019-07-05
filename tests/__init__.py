
def moduleInstalled(mod):
	try:
		__import__(mod)
		return True
	except ImportError:
		return False

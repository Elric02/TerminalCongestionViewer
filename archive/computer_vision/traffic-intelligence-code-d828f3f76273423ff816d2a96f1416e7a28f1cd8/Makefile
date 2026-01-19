INSTALL_DIR = /usr/local/bin
PYTHON_LIB_DIR = 

cexe:
	@cd c && make feature-based-tracking

doc:
	doxygen

clean:
	@cd c && make clean
	@cd trafficintelligence && rm *.pyc

installpython:
	@echo "========================================="
	@echo "Installing Python modules (in user space)"
	@tar cf /tmp/trafficintelligence.tar setup.py README trafficintelligence
	@gzip /tmp/trafficintelligence.tar
	@pip3 install /tmp/trafficintelligence.tar.gz
	@rm /tmp/trafficintelligence.tar.gz
installpythonscripts:
	@echo "========================================="
	@echo "Installing Python scripts"
	@cp scripts/*.py $(INSTALL_DIR)
uploadpython:
	@python setup.py sdist
	@twine upload dist/* --skip-existing --repository TRAFFIC_INTELLIGENCE
	#python setup.py sdist upload
	#python setup.py sdist bdist_wheel

install: cexe installpythonscripts
	@echo "========================================="
	@echo "Installing for Linux"
	@echo "========================================="
	@echo "Installing feature-based tracking executable"
	@cp bin/feature-based-tracking $(INSTALL_DIR)
uninstall:
	@echo "Uninstalling for Linux"
	rm $(INSTALL_DIR)/feature-based-tracking 
	@cd scripts && ./uninstall-scripts.sh $(INSTALL_DIR)

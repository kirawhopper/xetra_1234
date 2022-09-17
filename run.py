"""Running the Xetra ETL application"""
import logging
import logging.config
import yaml

def main():
    """
    Entry point to run the Xetra ETL Job
    """

    #parsing YAML file
    config_path = "configs/xetra_report1_config.yaml"
    config = yaml.safe_load(open(config_path))
    # Configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)
    logger.info("This is a test.")

if __name__ == "__main__":
    main()
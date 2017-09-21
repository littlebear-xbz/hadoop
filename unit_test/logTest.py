import logging
logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s:::] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    # filename='reply.log',
                    # filemode='w'
                    )

logging.info("a")
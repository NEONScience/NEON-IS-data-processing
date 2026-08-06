# Minimal mock for structlog to allow testing without installation
class Logger:
    def debug(self, msg):
        pass
    
    def error(self, msg):
        print(f"ERROR: {msg}")
    
    def info(self, msg):
        pass

def get_logger():
    return Logger()

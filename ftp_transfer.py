# ftp_transfer.py
import os
import json
import tempfile
from ftplib import FTP, FTP_TLS, all_errors as ftp_errors
import paramiko
import logging

class FTPTransfer:
    """
    Handles secure file transfers to FTP/SFTP servers
    """
    def __init__(self, config):
        """
        Initialize with connection configuration
        
        Args:
            config (dict): Dictionary containing FTP configuration
                - host: FTP server hostname
                - port: FTP server port (default 21 for FTP, 22 for SFTP)
                - username: FTP username
                - password: FTP password
                - path: Remote directory path
                - use_sftp: Boolean to use SFTP instead of FTP
                - use_ftps: Boolean to use FTPS (explicit TLS) instead of plain FTP
                - timeout: Connection timeout in seconds
        """
        self.config = config
        self.logger = logging.getLogger("FTPTransfer")
    
    def transfer(self, data, filename, file_format="json"):
        """
        Transfer data to the FTP server
        
        Args:
            data: The data to transfer (can be string, dict, etc.)
            filename: Name of the file on the remote server (without extension)
            file_format: Format of the file (json, xml, csv, etc.)
            
        Returns:
            tuple: (success, message)
        """
        try:
            # Create a temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=f".{file_format}") as temp:
                if file_format == "json":
                    if isinstance(data, str):
                        temp.write(data.encode('utf-8'))
                    else:
                        temp.write(json.dumps(data, indent=2).encode('utf-8'))
                else:
                    if isinstance(data, str):
                        temp.write(data.encode('utf-8'))
                    else:
                        temp.write(str(data).encode('utf-8'))
                
                temp_path = temp.name
            
            # Choose the appropriate transfer method
            if self.config.get("use_sftp", False):
                success, message = self._transfer_sftp(temp_path, f"{filename}.{file_format}")
            elif self.config.get("use_ftps", False):
                success, message = self._transfer_ftps(temp_path, f"{filename}.{file_format}")
            else:
                success, message = self._transfer_ftp(temp_path, f"{filename}.{file_format}")
            
            # Clean up the temporary file
            os.unlink(temp_path)
            return success, message
            
        except Exception as e:
            self.logger.error(f"Error in transfer: {str(e)}")
            return False, f"Transfer failed: {str(e)}"
    
    def _transfer_ftp(self, local_path, remote_filename):
        """
        Transfer file using standard FTP
        """
        try:
            with FTP() as ftp:
                # Connect and login
                ftp.connect(
                    host=self.config.get("host", "localhost"),
                    port=int(self.config.get("port", 21)),
                    timeout=int(self.config.get("timeout", 30))
                )
                ftp.login(
                    user=self.config.get("username", "anonymous"),
                    passwd=self.config.get("password", "")
                )
                
                # Navigate to the directory
                if self.config.get("path"):
                    try:
                        ftp.cwd(self.config["path"])
                    except ftp_errors as e:
                        # Create directory if it doesn't exist
                        dirs = self.config["path"].split('/')
                        current_dir = ""
                        for d in dirs:
                            if not d:
                                continue
                            current_dir += f"/{d}"
                            try:
                                ftp.cwd(current_dir)
                            except:
                                ftp.mkd(current_dir)
                                ftp.cwd(current_dir)
                
                # Upload file
                with open(local_path, 'rb') as file:
                    ftp.storbinary(f'STOR {remote_filename}', file)
                
                return True, f"Successfully transferred {remote_filename} via FTP"
        
        except ftp_errors as e:
            self.logger.error(f"FTP error: {str(e)}")
            return False, f"FTP error: {str(e)}"
        except Exception as e:
            self.logger.error(f"Error in FTP transfer: {str(e)}")
            return False, f"Error in FTP transfer: {str(e)}"
    
    def _transfer_ftps(self, local_path, remote_filename):
        """
        Transfer file using FTP with TLS (FTPS)
        """
        try:
            with FTP_TLS() as ftps:
                # Connect and login
                ftps.connect(
                    host=self.config.get("host", "localhost"),
                    port=int(self.config.get("port", 21)),
                    timeout=int(self.config.get("timeout", 30))
                )
                ftps.login(
                    user=self.config.get("username", "anonymous"),
                    passwd=self.config.get("password", "")
                )
                
                # Enable data protection
                ftps.prot_p()
                
                # Navigate to the directory
                if self.config.get("path"):
                    try:
                        ftps.cwd(self.config["path"])
                    except ftp_errors as e:
                        # Create directory if it doesn't exist
                        dirs = self.config["path"].split('/')
                        current_dir = ""
                        for d in dirs:
                            if not d:
                                continue
                            current_dir += f"/{d}"
                            try:
                                ftps.cwd(current_dir)
                            except:
                                ftps.mkd(current_dir)
                                ftps.cwd(current_dir)
                
                # Upload file
                with open(local_path, 'rb') as file:
                    ftps.storbinary(f'STOR {remote_filename}', file)
                
                return True, f"Successfully transferred {remote_filename} via FTPS"
        
        except ftp_errors as e:
            self.logger.error(f"FTPS error: {str(e)}")
            return False, f"FTPS error: {str(e)}"
        except Exception as e:
            self.logger.error(f"Error in FTPS transfer: {str(e)}")
            return False, f"Error in FTPS transfer: {str(e)}"
    
    def _transfer_sftp(self, local_path, remote_filename):
        """
        Transfer file using SFTP (SSH File Transfer Protocol)
        """
        transport = None
        sftp = None
        
        try:
            # Setup transport
            transport = paramiko.Transport((
                self.config.get("host", "localhost"),
                int(self.config.get("port", 22))
            ))
            transport.connect(
                username=self.config.get("username"),
                password=self.config.get("password")
            )
            
            # Create SFTP client
            sftp = paramiko.SFTPClient.from_transport(transport)
            
            # Make sure the remote directory exists
            remote_path = self.config.get("path", "")
            if remote_path:
                try:
                    sftp.stat(remote_path)
                except FileNotFoundError:
                    # Create directory structure
                    current_path = ""
                    for folder in remote_path.split("/"):
                        if not folder:
                            continue
                        
                        current_path += f"/{folder}"
                        try:
                            sftp.stat(current_path)
                        except FileNotFoundError:
                            sftp.mkdir(current_path)
            
            # Upload the file
            full_remote_path = f"{remote_path}/{remote_filename}" if remote_path else remote_filename
            sftp.put(local_path, full_remote_path)
            
            return True, f"Successfully transferred {remote_filename} via SFTP"
            
        except Exception as e:
            self.logger.error(f"SFTP error: {str(e)}")
            return False, f"SFTP error: {str(e)}"
            
        finally:
            # Clean up
            if sftp:
                sftp.close()
            if transport:
                transport.close()
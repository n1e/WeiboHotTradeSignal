#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日志系统模块
提供统一的日志记录功能
"""

import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from typing import Optional, Dict, Any


DEFAULT_CONFIG = {
    'level': 'INFO',
    'file': './logs/app.log',
    'max_size_mb': 10,
    'backup_count': 5
}


_logger_instance: Optional[logging.Logger] = None
_logger_config: Optional[Dict[str, Any]] = None


def get_log_level(level_str: str) -> int:
    """
    将字符串日志级别转换为 logging 模块的级别
    
    Args:
        level_str: 日志级别字符串（DEBUG, INFO, WARNING, ERROR, CRITICAL）
        
    Returns:
        对应的 logging 级别常量
    """
    level_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    return level_map.get(level_str.upper(), logging.INFO)


def setup_logger(config: Optional[Dict[str, Any]] = None) -> logging.Logger:
    """
    设置全局日志器
    
    Args:
        config: 日志配置字典
        
    Returns:
        配置好的日志器实例
    """
    global _logger_instance, _logger_config
    
    if config is None:
        config = DEFAULT_CONFIG.copy()
    else:
        merged_config = DEFAULT_CONFIG.copy()
        merged_config.update(config)
        config = merged_config
    
    _logger_config = config
    
    level = get_log_level(config.get('level', 'INFO'))
    log_file = config.get('file', './logs/app.log')
    max_size_mb = config.get('max_size_mb', 10)
    backup_count = config.get('backup_count', 5)
    
    logger = logging.getLogger('WeiboHotTradeSignal')
    logger.setLevel(level)
    
    if logger.handlers:
        logger.handlers.clear()
    
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    if log_file:
        try:
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)
            
            max_bytes = max_size_mb * 1024 * 1024
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=max_bytes,
                backupCount=backup_count,
                encoding='utf-8'
            )
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            logger.warning(f"无法创建文件日志处理器: {e}")
    
    _logger_instance = logger
    return logger


def get_logger() -> logging.Logger:
    """
    获取全局日志器实例
    
    Returns:
        日志器实例
    """
    global _logger_instance
    
    if _logger_instance is None:
        _logger_instance = setup_logger()
    
    return _logger_instance


def log_run_start(run_id: str, config: Dict[str, Any]):
    """
    记录任务运行开始
    
    Args:
        run_id: 运行ID
        config: 配置信息
    """
    logger = get_logger()
    logger.info("=" * 60)
    logger.info(f"任务开始执行 - 运行ID: {run_id}")
    logger.info(f"调度配置: 间隔={config.get('interval_minutes', 30)}分钟, "
                f"活跃时间={config.get('active_hours', '09:00-21:00')}")
    logger.info("=" * 60)


def log_run_end(run_id: str, success: bool, details: Optional[Dict[str, Any]] = None):
    """
    记录任务运行结束
    
    Args:
        run_id: 运行ID
        success: 是否成功
        details: 详细信息
    """
    logger = get_logger()
    status = "成功" if success else "失败"
    logger.info("=" * 60)
    logger.info(f"任务执行结束 - 运行ID: {run_id}, 状态: {status}")
    
    if details:
        for key, value in details.items():
            logger.info(f"  {key}: {value}")
    
    logger.info("=" * 60)


def log_step(step: str, message: str):
    """
    记录执行步骤
    
    Args:
        step: 步骤名称
        message: 步骤消息
    """
    logger = get_logger()
    logger.info(f"[{step}] {message}")


def log_error(error_type: str, error_msg: str, exception: Optional[Exception] = None):
    """
    记录错误信息
    
    Args:
        error_type: 错误类型
        error_msg: 错误消息
        exception: 异常对象（可选）
    """
    logger = get_logger()
    if exception:
        logger.error(f"[{error_type}] {error_msg}", exc_info=True)
    else:
        logger.error(f"[{error_type}] {error_msg}")


def log_push_result(pusher_name: str, success: bool, message: str):
    """
    记录推送结果
    
    Args:
        pusher_name: 推送器名称
        success: 是否成功
        message: 消息内容
    """
    logger = get_logger()
    status = "成功" if success else "失败"
    logger.info(f"[推送][{pusher_name}] {status} - {message}")


logger = get_logger()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
任务调度器模块
实现定时采集功能，支持间隔调度和活跃时间段配置
"""

import os
import sys
import time
import uuid
import signal
from datetime import datetime, time as dt_time
from typing import Optional, Dict, Any, Callable
from threading import Event

from logger import logger, log_run_start, log_run_end, log_step, log_error


class TaskScheduler:
    """任务调度器"""
    
    def __init__(self, config: Dict[str, Any], task_func: Optional[Callable] = None):
        """
        初始化任务调度器
        
        Args:
            config: 调度配置
            task_func: 要执行的任务函数
        """
        self.config = config
        self.task_func = task_func
        
        self.enabled = config.get('enabled', True)
        self.interval_minutes = config.get('interval_minutes', 30)
        self.active_hours = config.get('active_hours', '09:00-21:00')
        self.timezone = config.get('timezone', 'Asia/Shanghai')
        
        self._stop_event = Event()
        self._running = False
        self._last_run_id = None
        
        self._parse_active_hours()
        self._setup_signal_handlers()
    
    def _parse_active_hours(self):
        """解析活跃时间段"""
        try:
            if '-' in self.active_hours:
                start_str, end_str = self.active_hours.split('-', 1)
                self.active_start = self._parse_time(start_str.strip())
                self.active_end = self._parse_time(end_str.strip())
            else:
                self.active_start = dt_time(9, 0)
                self.active_end = dt_time(21, 0)
            
            logger.info(f"活跃时间段配置: {self.active_start} - {self.active_end}")
        except Exception as e:
            logger.error(f"解析活跃时间段失败: {e}，使用默认值 09:00-21:00")
            self.active_start = dt_time(9, 0)
            self.active_end = dt_time(21, 0)
    
    def _parse_time(self, time_str: str) -> dt_time:
        """
        解析时间字符串
        
        Args:
            time_str: 时间字符串，如 "09:00" 或 "9:00"
            
        Returns:
            datetime.time 对象
        """
        parts = time_str.strip().split(':')
        if len(parts) >= 2:
            hour = int(parts[0])
            minute = int(parts[1])
            return dt_time(hour, minute)
        elif len(parts) == 1:
            hour = int(parts[0])
            return dt_time(hour, 0)
        else:
            raise ValueError(f"无效的时间格式: {time_str}")
    
    def _setup_signal_handlers(self):
        """设置信号处理器"""
        def handle_signal(signum, frame):
            logger.info(f"收到信号 {signum}，准备停止调度器...")
            self.stop()
        
        signal.signal(signal.SIGINT, handle_signal)
        signal.signal(signal.SIGTERM, handle_signal)
    
    def is_in_active_hours(self) -> bool:
        """
        检查当前时间是否在活跃时间段内
        
        Returns:
            是否在活跃时间段内
        """
        now = datetime.now().time()
        
        if self.active_start <= self.active_end:
            return self.active_start <= now <= self.active_end
        else:
            return now >= self.active_start or now <= self.active_end
    
    def get_next_run_time(self) -> datetime:
        """
        计算下次运行时间
        
        Returns:
            下次运行时间
        """
        now = datetime.now()
        interval_seconds = self.interval_minutes * 60
        
        next_run = now.replace(second=0, microsecond=0)
        
        while next_run <= now:
            from datetime import timedelta
            next_run += timedelta(seconds=interval_seconds)
        
        return next_run
    
    def generate_run_id(self) -> str:
        """
        生成运行ID
        
        Returns:
            运行ID字符串
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        short_uuid = str(uuid.uuid4())[:8]
        return f"run_{timestamp}_{short_uuid}"
    
    def run_task(self) -> Dict[str, Any]:
        """
        执行一次任务
        
        Returns:
            任务执行结果
        """
        run_id = self.generate_run_id()
        self._last_run_id = run_id
        
        start_time = datetime.now()
        
        result = {
            'run_id': run_id,
            'start_time': start_time.isoformat(),
            'end_time': None,
            'success': False,
            'details': {}
        }
        
        try:
            log_run_start(run_id, self.config)
            
            if self.task_func:
                log_step("任务执行", "开始执行任务函数...")
                task_result = self.task_func()
                
                result['success'] = True
                result['details']['task_result'] = task_result
                
                log_step("任务执行", "任务函数执行完成")
            else:
                logger.warning("未设置任务函数，仅执行空运行")
                result['success'] = True
                result['details']['message'] = "空运行，未设置任务函数"
            
        except Exception as e:
            result['success'] = False
            result['details']['error'] = str(e)
            log_error("任务执行", f"任务执行失败: {e}", e)
        
        end_time = datetime.now()
        result['end_time'] = end_time.isoformat()
        
        duration = (end_time - start_time).total_seconds()
        result['duration_seconds'] = duration
        result['details']['duration'] = f"{duration:.2f}秒"
        
        log_run_end(run_id, result['success'], result['details'])
        
        return result
    
    def start(self):
        """启动调度器"""
        if not self.enabled:
            logger.info("调度器已禁用，退出")
            return
        
        logger.info("=" * 60)
        logger.info("任务调度器启动")
        logger.info(f"配置: 间隔={self.interval_minutes}分钟, 活跃时间={self.active_hours}")
        logger.info("=" * 60)
        
        self._running = True
        self._stop_event.clear()
        
        first_run = True
        
        while self._running and not self._stop_event.is_set():
            try:
                if first_run:
                    if self.is_in_active_hours():
                        logger.info("首次运行：当前在活跃时间段内，立即执行任务")
                        self.run_task()
                    else:
                        logger.info("首次运行：当前不在活跃时间段内，等待到下一次调度时间")
                    first_run = False
                
                next_run = self.get_next_run_time()
                now = datetime.now()
                wait_seconds = (next_run - now).total_seconds()
                
                if wait_seconds > 0:
                    logger.info(f"下次运行时间: {next_run.strftime('%Y-%m-%d %H:%M:%S')} (等待 {wait_seconds:.0f} 秒)")
                    
                    self._stop_event.wait(wait_seconds)
                    
                    if self._stop_event.is_set():
                        logger.info("收到停止信号，退出调度循环")
                        break
                
                if self.is_in_active_hours():
                    logger.info("当前在活跃时间段内，开始执行任务")
                    self.run_task()
                else:
                    logger.info("当前不在活跃时间段内，跳过本次任务")
                    logger.info(f"活跃时间段: {self.active_start} - {self.active_end}")
                    logger.info(f"当前时间: {datetime.now().time()}")
                
            except Exception as e:
                log_error("调度器", f"调度循环发生异常: {e}", e)
                logger.info("等待 60 秒后继续...")
                self._stop_event.wait(60)
        
        logger.info("任务调度器已停止")
    
    def stop(self):
        """停止调度器"""
        logger.info("正在停止调度器...")
        self._running = False
        self._stop_event.set()
    
    def run_once(self) -> Dict[str, Any]:
        """
        仅执行一次任务（用于手动触发）
        
        Returns:
            任务执行结果
        """
        logger.info("执行单次任务模式")
        return self.run_task()


def run_with_scheduler(config: Dict[str, Any], task_func: Callable, 
                       use_scheduler: bool = True) -> Optional[Dict[str, Any]]:
    """
    使用调度器运行任务
    
    Args:
        config: 完整配置
        task_func: 任务函数
        use_scheduler: 是否使用调度器
        
    Returns:
        单次执行时返回结果，调度模式返回 None
    """
    schedule_config = config.get('schedule', {})
    scheduler = TaskScheduler(schedule_config, task_func)
    
    if use_scheduler and scheduler.enabled:
        scheduler.start()
        return None
    else:
        logger.info("使用单次执行模式")
        return scheduler.run_once()

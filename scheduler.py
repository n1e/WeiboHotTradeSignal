#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
定时任务调度模块 - 用于执行每日和每周热门话题总结任务
"""

import json
import os
import sys
import time
import logging
import signal
from datetime import datetime, date, timedelta
from typing import Dict, Optional

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED
    APSCHEDULER_AVAILABLE = True
except ImportError:
    APSCHEDULER_AVAILABLE = False
    print("警告: apscheduler 未安装，定时调度功能不可用")
    print("请运行: pip install apscheduler")

from topic_summarizer import TopicSummarizer


class TopicScheduler:
    """热门话题定时任务调度器"""
    
    def __init__(self, config: Dict):
        """
        初始化调度器
        
        Args:
            config: 配置字典
        """
        self.config = config
        self.logger = self._setup_logger()
        
        self.summarizer = TopicSummarizer(config)
        
        self.scheduler_config = config.get('schedule', {})
        self.summary_config = config.get('summary', {})
        
        self.daily_cron = self.summary_config.get('daily_cron', '0 22 * * *')
        self.weekly_cron = self.summary_config.get('weekly_cron', '0 23 * * 0')
        self.timezone = self.scheduler_config.get('timezone', 'Asia/Shanghai')
        
        self.scheduler = None
        self.running = False
    
    def _setup_logger(self) -> logging.Logger:
        """设置日志记录器"""
        logger = logging.getLogger('TopicScheduler')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            
            logger.addHandler(ch)
            
            log_config = self.config.get('logging', {})
            log_file = log_config.get('file', './logs/scheduler.log')
            if log_file:
                os.makedirs(os.path.dirname(log_file), exist_ok=True)
                fh = logging.FileHandler(log_file, encoding='utf-8')
                fh.setLevel(logging.INFO)
                fh.setFormatter(formatter)
                logger.addHandler(fh)
        
        return logger
    
    def run_daily_task(self, target_date: date = None) -> bool:
        """
        执行每日热门话题总结任务
        
        Args:
            target_date: 目标日期，默认为当天
            
        Returns:
            是否执行成功
        """
        if target_date is None:
            target_date = date.today()
        
        self.logger.info(f"开始执行每日热门话题总结任务，目标日期: {target_date}")
        
        try:
            success, topics = self.summarizer.summarize_daily_topics(target_date)
            
            if success and topics:
                self.logger.info(f"每日热门话题总结任务执行成功，共识别出 {len(topics)} 个热门话题")
                return True
            else:
                self.logger.warning(f"每日热门话题总结任务执行完成但未识别出话题，日期: {target_date}")
                return False
                
        except Exception as e:
            self.logger.error(f"每日热门话题总结任务执行失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False
    
    def run_weekly_task(self, week_start: date = None, week_end: date = None) -> bool:
        """
        执行每周热门话题总结任务
        
        Args:
            week_start: 周开始日期，默认为本周一
            week_end: 周结束日期，默认为本周日
            
        Returns:
            是否执行成功
        """
        if week_start is None or week_end is None:
            today = date.today()
            week_start = today - timedelta(days=today.weekday())
            week_end = week_start + timedelta(days=6)
        
        self.logger.info(f"开始执行每周热门话题总结任务，目标周: {week_start} ~ {week_end}")
        
        try:
            success, topics = self.summarizer.summarize_weekly_topics(week_start, week_end)
            
            if success and topics:
                self.logger.info(f"每周热门话题总结任务执行成功，共识别出 {len(topics)} 个热门话题")
                return True
            else:
                self.logger.warning(f"每周热门话题总结任务执行完成但未识别出话题，周: {week_start} ~ {week_end}")
                return False
                
        except Exception as e:
            self.logger.error(f"每周热门话题总结任务执行失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False
    
    def _on_job_error(self, event):
        """定时任务错误处理"""
        self.logger.error(f"定时任务执行出错: {event.job_id}")
        if event.exception:
            self.logger.error(f"异常信息: {event.exception}")
        if event.traceback:
            self.logger.error(event.traceback)
    
    def _on_job_missed(self, event):
        """定时任务错过处理"""
        self.logger.warning(f"定时任务被错过: {event.job_id}")
    
    def start_scheduler(self):
        """启动定时任务调度器"""
        if not APSCHEDULER_AVAILABLE:
            self.logger.error("apscheduler 未安装，无法启动定时调度器")
            return False
        
        if self.running:
            self.logger.warning("调度器已经在运行中")
            return True
        
        self.logger.info("=" * 60)
        self.logger.info("启动热门话题定时任务调度器")
        self.logger.info(f"每日任务 Cron 表达式: {self.daily_cron}")
        self.logger.info(f"每周任务 Cron 表达式: {self.weekly_cron}")
        self.logger.info(f"时区: {self.timezone}")
        self.logger.info("=" * 60)
        
        try:
            self.scheduler = BackgroundScheduler(timezone=self.timezone)
            
            self.scheduler.add_listener(self._on_job_error, EVENT_JOB_ERROR)
            self.scheduler.add_listener(self._on_job_missed, EVENT_JOB_MISSED)
            
            daily_trigger = CronTrigger.from_crontab(self.daily_cron)
            self.scheduler.add_job(
                self.run_daily_task,
                trigger=daily_trigger,
                id='daily_hot_topic_summary',
                name='每日热门话题总结',
                replace_existing=True
            )
            self.logger.info(f"已添加每日热门话题总结任务，触发时间: {self.daily_cron}")
            
            weekly_trigger = CronTrigger.from_crontab(self.weekly_cron)
            self.scheduler.add_job(
                self.run_weekly_task,
                trigger=weekly_trigger,
                id='weekly_hot_topic_summary',
                name='每周热门话题总结',
                replace_existing=True
            )
            self.logger.info(f"已添加每周热门话题总结任务，触发时间: {self.weekly_cron}")
            
            self.scheduler.start()
            self.running = True
            
            self.logger.info("定时任务调度器已启动")
            self.logger.info("按 Ctrl+C 停止调度器")
            
            return True
            
        except Exception as e:
            self.logger.error(f"启动调度器失败: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False
    
    def stop_scheduler(self):
        """停止定时任务调度器"""
        if not self.running:
            return
        
        self.logger.info("正在停止定时任务调度器...")
        
        try:
            if self.scheduler:
                self.scheduler.shutdown(wait=False)
            self.running = False
            self.logger.info("定时任务调度器已停止")
        except Exception as e:
            self.logger.error(f"停止调度器时出错: {e}")
    
    def wait_forever(self):
        """无限等待，保持调度器运行"""
        def signal_handler(signum, frame):
            self.logger.info("收到停止信号，正在关闭...")
            self.stop_scheduler()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("收到键盘中断，正在关闭...")
            self.stop_scheduler()


def load_config(config_path: str = 'config.json') -> Optional[Dict]:
    """加载配置文件"""
    if not os.path.exists(config_path):
        print(f"配置文件不存在: {config_path}")
        print(f"请复制 config.example.json 为 config.json 并填写配置信息")
        return None
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config
    except json.JSONDecodeError as e:
        print(f"配置文件格式错误: {e}")
        return None
    except Exception as e:
        print(f"加载配置文件失败: {e}")
        return None


def check_config(config: Dict) -> bool:
    """检查配置是否完整"""
    errors = []
    
    openrouter_config = config.get('openrouter', {})
    if not openrouter_config.get('api_key'):
        errors.append("缺少OpenRouter API密钥配置")
    
    if errors:
        print("配置检查失败:")
        for error in errors:
            print(f"  - {error}")
        return False
    
    return True


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='热门话题定时任务调度器')
    parser.add_argument('-c', '--config', default='config.json', help='配置文件路径')
    parser.add_argument('--mode', choices=['schedule', 'daily', 'weekly'], required=True,
                        help='运行模式：schedule(定时调度)、daily(单次每日任务)、weekly(单次每周任务)')
    parser.add_argument('--date', help='指定日期（daily模式时使用），格式：YYYY-MM-DD')
    parser.add_argument('--week-start', help='周开始日期（weekly模式时使用），格式：YYYY-MM-DD')
    parser.add_argument('--week-end', help='周结束日期（weekly模式时使用），格式：YYYY-MM-DD')
    parser.add_argument('--skip-config-check', action='store_true', help='跳过配置检查（仅用于测试）')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("热门话题定时任务调度器")
    print(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"运行模式: {args.mode}")
    print("=" * 60)
    
    config = load_config(args.config)
    if not config:
        print("配置加载失败，程序退出")
        sys.exit(1)
    
    print(f"配置文件加载成功: {args.config}")
    
    if not args.skip_config_check and not check_config(config):
        print("配置不完整，程序退出")
        sys.exit(1)
    
    scheduler = TopicScheduler(config)
    
    if args.mode == 'schedule':
        if not APSCHEDULER_AVAILABLE:
            print("错误: apscheduler 未安装，无法使用定时调度模式")
            print("请运行: pip install apscheduler")
            sys.exit(1)
        
        success = scheduler.start_scheduler()
        if success:
            scheduler.wait_forever()
        else:
            print("启动调度器失败")
            sys.exit(1)
    
    elif args.mode == 'daily':
        target_date = None
        if args.date:
            try:
                target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
                print(f"指定日期: {target_date}")
            except ValueError:
                print(f"日期格式错误: {args.date}，请使用 YYYY-MM-DD 格式")
                sys.exit(1)
        
        success = scheduler.run_daily_task(target_date)
        
        if success:
            print("\n每日热门话题总结任务执行成功！")
            sys.exit(0)
        else:
            print("\n每日热门话题总结任务执行失败或未识别出话题")
            sys.exit(1)
    
    elif args.mode == 'weekly':
        week_start = None
        week_end = None
        
        if args.week_start:
            try:
                week_start = datetime.strptime(args.week_start, '%Y-%m-%d').date()
                print(f"周开始日期: {week_start}")
            except ValueError:
                print(f"周开始日期格式错误: {args.week_start}，请使用 YYYY-MM-DD 格式")
                sys.exit(1)
        
        if args.week_end:
            try:
                week_end = datetime.strptime(args.week_end, '%Y-%m-%d').date()
                print(f"周结束日期: {week_end}")
            except ValueError:
                print(f"周结束日期格式错误: {args.week_end}，请使用 YYYY-MM-DD 格式")
                sys.exit(1)
        
        success = scheduler.run_weekly_task(week_start, week_end)
        
        if success:
            print("\n每周热门话题总结任务执行成功！")
            sys.exit(0)
        else:
            print("\n每周热门话题总结任务执行失败或未识别出话题")
            sys.exit(1)


if __name__ == '__main__':
    main()

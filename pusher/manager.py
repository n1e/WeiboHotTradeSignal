#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
推送管理器
统一管理所有推送器
"""

from typing import Optional, Dict, List, Any

from logger import logger
from pusher.base import BasePusher
from pusher.feishu import FeishuPusher


class PushManager:
    """推送管理器"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        初始化推送管理器
        
        Args:
            config: 推送配置
        """
        self.config = config or {}
        self._pushers: Dict[str, BasePusher] = {}
        self._init_pushers()
    
    def _init_pushers(self):
        """初始化所有推送器"""
        push_config = self.config
        
        if not push_config.get('enabled', False):
            logger.info("推送功能已禁用，跳过初始化推送器")
            return
        
        feishu_config = push_config.get('feishu', {})
        if feishu_config.get('enabled', False):
            try:
                self._pushers['feishu'] = FeishuPusher(feishu_config)
                logger.info("飞书推送器初始化成功")
            except Exception as e:
                logger.error(f"飞书推送器初始化失败: {e}")
        
        logger.info(f"已初始化 {len(self._pushers)} 个推送器")
    
    def get_pusher(self, name: str) -> Optional[BasePusher]:
        """
        获取指定名称的推送器
        
        Args:
            name: 推送器名称（'feishu'）
            
        Returns:
            推送器实例，不存在返回 None
        """
        return self._pushers.get(name)
    
    def get_all_pushers(self) -> List[BasePusher]:
        """
        获取所有推送器
        
        Returns:
            推送器列表
        """
        return list(self._pushers.values())
    
    def push(self, 
             title: str, 
             content: str, 
             file_path: Optional[str] = None,
             pusher_names: Optional[List[str]] = None) -> Dict[str, bool]:
        """
        推送消息到所有或指定的推送器
        
        Args:
            title: 消息标题
            content: 消息内容
            file_path: 附件文件路径（可选）
            pusher_names: 指定推送器名称列表，None 表示所有
            
        Returns:
            每个推送器的推送结果字典
        """
        results = {}
        
        if pusher_names:
            pushers = [self._pushers[name] for name in pusher_names if name in self._pushers]
        else:
            pushers = self.get_all_pushers()
        
        if not pushers:
            logger.warning("没有可用的推送器")
            return results
        
        for pusher in pushers:
            pusher_name = type(pusher).__name__
            try:
                success = pusher.push(title, content, file_path)
                results[pusher_name] = success
                if success:
                    logger.info(f"{pusher_name} 推送成功")
                else:
                    logger.warning(f"{pusher_name} 推送失败")
            except Exception as e:
                logger.error(f"{pusher_name} 推送异常: {e}")
                results[pusher_name] = False
        
        return results
    
    def push_file(self, 
                  file_path: str, 
                  title: Optional[str] = None,
                  pusher_names: Optional[List[str]] = None) -> Dict[str, bool]:
        """
        推送文件到所有或指定的推送器
        
        Args:
            file_path: 文件路径
            title: 文件标题（可选）
            pusher_names: 指定推送器名称列表，None 表示所有
            
        Returns:
            每个推送器的推送结果字典
        """
        results = {}
        
        if pusher_names:
            pushers = [self._pushers[name] for name in pusher_names if name in self._pushers]
        else:
            pushers = self.get_all_pushers()
        
        if not pushers:
            logger.warning("没有可用的推送器")
            return results
        
        for pusher in pushers:
            pusher_name = type(pusher).__name__
            try:
                success = pusher.push_file(file_path, title)
                results[pusher_name] = success
                if success:
                    logger.info(f"{pusher_name} 文件推送成功: {file_path}")
                else:
                    logger.warning(f"{pusher_name} 文件推送失败: {file_path}")
            except Exception as e:
                logger.error(f"{pusher_name} 文件推送异常: {e}")
                results[pusher_name] = False
        
        return results
    
    def push_analysis_card(self,
                           title: str,
                           analysis_result: Dict[str, Any],
                           html_path: Optional[str] = None,
                           pusher_names: Optional[List[str]] = None) -> Dict[str, bool]:
        """
        推送分析结果卡片到所有或指定的推送器
        
        Args:
            title: 消息标题
            analysis_result: 分析结果
            html_path: HTML报告路径（可选）
            pusher_names: 指定推送器名称列表，None 表示所有
            
        Returns:
            每个推送器的推送结果字典
        """
        results = {}
        
        if pusher_names:
            pushers = [self._pushers[name] for name in pusher_names if name in self._pushers]
        else:
            pushers = self.get_all_pushers()
        
        if not pushers:
            logger.warning("没有可用的推送器")
            return results
        
        for pusher in pushers:
            pusher_name = type(pusher).__name__
            try:
                success = pusher.push_card(title, analysis_result, html_path)
                results[pusher_name] = success
                if success:
                    logger.info(f"{pusher_name} 分析卡片推送成功")
                else:
                    logger.warning(f"{pusher_name} 分析卡片推送失败")
            except Exception as e:
                logger.error(f"{pusher_name} 分析卡片推送异常: {e}")
                results[pusher_name] = False
        
        return results
    
    def is_available(self) -> bool:
        """
        检查是否有可用的推送器
        
        Returns:
            是否有可用的推送器
        """
        return len(self._pushers) > 0
    
    def reload(self) -> int:
        """
        重新初始化所有推送器
        
        Returns:
            成功初始化的推送器数量
        """
        logger.info("正在重新初始化推送器...")
        
        old_count = len(self._pushers)
        self._pushers.clear()
        
        self._init_pushers()
        
        new_count = len(self._pushers)
        logger.info(f"推送器重新初始化完成: {old_count} -> {new_count}")
        
        return new_count


_push_manager_instance: Optional[PushManager] = None


def get_push_manager(config: Optional[Dict[str, Any]] = None) -> PushManager:
    """
    获取全局推送管理器实例（单例模式）
    
    Args:
        config: 推送配置
        
    Returns:
        推送管理器实例
    """
    global _push_manager_instance
    if _push_manager_instance is None:
        _push_manager_instance = PushManager(config)
    return _push_manager_instance


def reset_push_manager():
    """
    重置推送管理器实例
    """
    global _push_manager_instance
    _push_manager_instance = None
    logger.info("推送管理器实例已重置")

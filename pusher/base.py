#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
推送基类
定义推送接口
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List


class BasePusher(ABC):
    """推送基类"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化推送器
        
        Args:
            config: 推送配置
        """
        self.config = config
        self.enabled = config.get('enabled', False)
    
    @abstractmethod
    def push(self, title: str, content: str, file_path: Optional[str] = None) -> bool:
        """
        推送消息
        
        Args:
            title: 消息标题
            content: 消息内容
            file_path: 附件文件路径（可选）
            
        Returns:
            是否推送成功
        """
        pass
    
    @abstractmethod
    def push_file(self, file_path: str, title: Optional[str] = None) -> bool:
        """
        推送文件
        
        Args:
            file_path: 文件路径
            title: 文件标题（可选）
            
        Returns:
            是否推送成功
        """
        pass
    
    @abstractmethod
    def push_card(self, title: str, analysis_result: Dict[str, Any], 
                  html_path: Optional[str] = None) -> bool:
        """
        推送分析卡片消息
        
        Args:
            title: 消息标题
            analysis_result: 分析结果
            html_path: HTML报告路径（可选）
            
        Returns:
            是否推送成功
        """
        pass
    
    @abstractmethod
    def push_alert(self, alerts: List[Dict[str, Any]]) -> bool:
        """
        推送预警消息
        
        Args:
            alerts: 预警事件列表
            
        Returns:
            是否推送成功
        """
        pass

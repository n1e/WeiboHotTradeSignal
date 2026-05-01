#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
飞书推送器
实现飞书消息推送功能 - 企业自建应用方式
"""

import os
import json
import time
from typing import Optional, Dict, Any, List
import requests

from logger import logger
from pusher.base import BasePusher


class FeishuPusher(BasePusher):
    """飞书推送器"""
    
    TOKEN_URL = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    UPLOAD_URL = "https://open.feishu.cn/open-apis/im/v1/files"
    SEND_URL = "https://open.feishu.cn/open-apis/im/v1/messages"
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化飞书推送器
        
        Args:
            config: 飞书配置，包含 app_id, app_secret, chat_id
        """
        super().__init__(config)
        self.app_id = config.get('app_id', '')
        self.app_secret = config.get('app_secret', '')
        self.chat_id = config.get('chat_id', '')
        
        if not self.app_id:
            self.app_id = os.environ.get('FS_ID', '')
        if not self.app_secret:
            self.app_secret = os.environ.get('FS_KEY', '')
        if not self.chat_id:
            self.chat_id = os.environ.get('FS_CHAT_ID', '')
        
        self._access_token = None
        self._token_expire_time = 0
        
        self._check_config()
    
    def _check_config(self):
        """检查配置是否完整"""
        if not self.app_id or not self.app_secret or not self.chat_id:
            logger.warning("飞书配置不完整，需要 app_id, app_secret, chat_id")
            self.enabled = False
        else:
            logger.info(f"飞书推送器初始化完成，chat_id: {self.chat_id}")
    
    def _get_access_token(self) -> Optional[str]:
        """
        获取飞书访问令牌
        
        Returns:
            访问令牌，失败返回 None
        """
        if self._access_token and time.time() < self._token_expire_time:
            return self._access_token
        
        try:
            logger.info("获取飞书访问令牌...")
            payload = {
                "app_id": self.app_id,
                "app_secret": self.app_secret
            }
            
            response = requests.post(self.TOKEN_URL, json=payload, timeout=10)
            response.raise_for_status()
            
            result = response.json()
            if result.get('code') == 0:
                self._access_token = result.get('tenant_access_token')
                expire = result.get('expire', 7200)
                self._token_expire_time = time.time() + expire - 300
                logger.info("飞书访问令牌获取成功")
                return self._access_token
            else:
                logger.error(f"获取飞书访问令牌失败: {result.get('msg', '未知错误')}")
                return None
                
        except Exception as e:
            logger.error(f"获取飞书访问令牌异常: {e}")
            return None
    
    def _upload_file(self, file_path: str) -> Optional[str]:
        """
        上传文件到飞书
        
        Args:
            file_path: 文件路径
            
        Returns:
            文件 key，失败返回 None
        """
        token = self._get_access_token()
        if not token:
            return None
        
        try:
            import os
            file_name = os.path.basename(file_path)
            file_size = os.path.getsize(file_path)
            
            logger.info(f"上传文件到飞书: {file_name} ({file_size} 字节)")
            
            headers = {
                'Authorization': f'Bearer {token}'
            }
            
            with open(file_path, 'rb') as f:
                files = {
                    'file': (file_name, f, 'text/html' if file_name.endswith('.html') else 'application/octet-stream')
                }
                
                data = {
                    'file_type': 'stream',
                    'file_name': file_name
                }
                
                response = requests.post(
                    self.UPLOAD_URL,
                    headers=headers,
                    data=data,
                    files=files,
                    timeout=60
                )
                response.raise_for_status()
                
                result = response.json()
                if result.get('code') == 0:
                    file_key = result.get('data', {}).get('file_key')
                    logger.info(f"文件上传成功，file_key: {file_key}")
                    return file_key
                else:
                    logger.error(f"文件上传失败: {result.get('msg', '未知错误')}")
                    return None
                    
        except Exception as e:
            logger.error(f"上传文件到飞书异常: {e}")
            return None
    
    def _send_file_message(self, file_key: str, title: Optional[str] = None) -> bool:
        """
        发送文件消息到飞书群
        
        Args:
            file_key: 文件 key
            title: 消息标题（可选）
            
        Returns:
            是否发送成功
        """
        token = self._get_access_token()
        if not token:
            return False
        
        try:
            logger.info(f"发送文件消息到飞书群: {self.chat_id}")
            
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            content = json.dumps({
                'file_key': file_key
            })
            
            payload = {
                'receive_id': self.chat_id,
                'msg_type': 'file',
                'content': content
            }
            
            params = {
                'receive_id_type': 'chat_id'
            }
            
            response = requests.post(
                self.SEND_URL,
                headers=headers,
                params=params,
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            
            result = response.json()
            if result.get('code') == 0:
                message_id = result.get('data', {}).get('message_id')
                logger.info(f"消息发送成功，message_id: {message_id}")
                return True
            else:
                logger.error(f"消息发送失败: {result.get('msg', '未知错误')}")
                return False
                
        except Exception as e:
            logger.error(f"发送飞书消息异常: {e}")
            return False
    
    def _send_text_message(self, title: str, content: str) -> bool:
        """
        发送文本消息
        
        Args:
            title: 标题
            content: 内容
            
        Returns:
            是否发送成功
        """
        token = self._get_access_token()
        if not token:
            return False
        
        try:
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            full_content = f"📢 {title}\n\n{content}"
            
            payload = {
                'receive_id': self.chat_id,
                'msg_type': 'text',
                'content': json.dumps({'text': full_content})
            }
            
            params = {
                'receive_id_type': 'chat_id'
            }
            
            response = requests.post(
                self.SEND_URL,
                headers=headers,
                params=params,
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            
            result = response.json()
            if result.get('code') == 0:
                logger.info("文本消息发送成功")
                return True
            else:
                logger.error(f"文本消息发送失败: {result.get('msg', '未知错误')}")
                return False
                
        except Exception as e:
            logger.error(f"发送文本消息异常: {e}")
            return False
    
    def _build_analysis_card_content(self, title: str, analysis_result: Dict[str, Any]) -> str:
        """
        构建分析结果卡片内容（用于富文本消息）
        
        Args:
            title: 消息标题
            analysis_result: 分析结果
            
        Returns:
            富文本消息内容
        """
        trend_analysis = analysis_result.get('trend_analysis', {})
        stock_analysis = analysis_result.get('stock_analysis', {})
        
        new_hot_topics = trend_analysis.get('new_hot_topics', [])
        rising_topics = trend_analysis.get('rising_topics', [])
        stock_opportunities = stock_analysis.get('stock_opportunities', [])
        market_sentiment = stock_analysis.get('market_sentiment', {})
        
        lines = []
        lines.append(f"📊 **{title}**")
        lines.append(f"生成时间: {analysis_result.get('timestamp', '未知')}")
        lines.append("")
        
        if new_hot_topics:
            lines.append("🆕 **新增热搜**")
            for t in new_hot_topics[:3]:
                lines.append(f"• {t.get('title', '未知')}")
                lines.append(f"  {t.get('reason', '')}")
            lines.append("")
        
        if rising_topics:
            lines.append("📈 **热度上升话题**")
            for t in rising_topics[:3]:
                lines.append(f"• {t.get('title', '未知')}")
                lines.append(f"  趋势: {t.get('trend', '')}")
                lines.append(f"  影响: {t.get('potential_impact', '')}")
            lines.append("")
        
        if stock_opportunities:
            lines.append("💼 **LLM挖掘的潜在题材与股票建议**")
            for opp in stock_opportunities[:2]:
                lines.append(f"🎯 {opp.get('event', '未知事件')}")
                lines.append(f"   影响程度: {opp.get('impact_level', '中')}")
                lines.append(f"   关联行业: {', '.join(opp.get('related_industries', []))}")
                
                stocks = opp.get('related_stocks', [])
                if stocks:
                    lines.append("   相关股票:")
                    for s in stocks[:2]:
                        lines.append(f"   - {s.get('stock_name', '')}({s.get('stock_code', '')}) "
                                    f"[{s.get('signal_type', '')} 信心:{s.get('confidence', 0)}%]")
                lines.append("")
        
        if market_sentiment:
            lines.append("📊 **市场情绪**")
            lines.append(f"   整体情绪: {market_sentiment.get('overall_sentiment', '谨慎')}")
            lines.append(f"   热门行业: {', '.join(market_sentiment.get('hot_industries', []))}")
            lines.append("")
        
        lines.append("⚠️ 免责声明：本报告仅供参考，不构成任何投资建议。")
        lines.append("   股市有风险，投资需谨慎。")
        
        return "\n".join(lines)
    
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
        if not self.enabled:
            logger.info("飞书推送已禁用，跳过")
            return False
        
        if not self.app_id or not self.app_secret or not self.chat_id:
            logger.warning("飞书配置不完整，无法推送")
            return False
        
        if file_path:
            return self.push_file(file_path, title)
        
        return self._send_text_message(title, content)
    
    def push_file(self, file_path: str, title: Optional[str] = None) -> bool:
        """
        推送文件
        
        Args:
            file_path: 文件路径
            title: 文件标题（可选）
            
        Returns:
            是否推送成功
        """
        if not self.enabled:
            logger.info("飞书推送已禁用，跳过")
            return False
        
        if not self.app_id or not self.app_secret or not self.chat_id:
            logger.warning("飞书配置不完整，无法推送")
            return False
        
        import os
        if not os.path.exists(file_path):
            logger.error(f"文件不存在: {file_path}")
            return False
        
        file_key = self._upload_file(file_path)
        if not file_key:
            return False
        
        return self._send_file_message(file_key, title)
    
    def push_card(self, title: str, analysis_result: Dict[str, Any], 
                  html_path: Optional[str] = None) -> bool:
        """
        推送分析结果消息
        
        Args:
            title: 消息标题
            analysis_result: 分析结果
            html_path: HTML报告路径（可选，将作为文件推送）
            
        Returns:
            是否推送成功
        """
        if not self.enabled:
            logger.info("飞书推送已禁用，跳过")
            return False
        
        if not self.app_id or not self.app_secret or not self.chat_id:
            logger.warning("飞书配置不完整，无法推送")
            return False
        
        success = True
        
        content = self._build_analysis_card_content(title, analysis_result)
        text_success = self._send_text_message(title, content)
        if not text_success:
            success = False
        
        if html_path:
            logger.info("推送HTML报告文件...")
            file_success = self.push_file(html_path, f"HTML报告: {title}")
            if not file_success:
                success = False
        
        return success

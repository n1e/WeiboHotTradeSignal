#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
飞书推送器
实现飞书消息推送功能，支持 Webhook 和应用两种方式
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
            config: 飞书配置，包含 app_id, app_secret, chat_id, webhook_url
        """
        super().__init__(config)
        self.app_id = config.get('app_id', '')
        self.app_secret = config.get('app_secret', '')
        self.chat_id = config.get('chat_id', '')
        self.webhook_url = config.get('webhook_url', '')
        
        if not self.app_id:
            self.app_id = os.environ.get('FS_ID', '')
        if not self.app_secret:
            self.app_secret = os.environ.get('FS_KEY', '')
        if not self.chat_id:
            self.chat_id = os.environ.get('FS_CHAT_ID', '')
        if not self.webhook_url:
            self.webhook_url = os.environ.get('FS_WEBHOOK_URL', '')
        
        self._access_token = None
        self._token_expire_time = 0
        
        self._check_mode()
    
    def _check_mode(self):
        """检查推送模式"""
        if self.webhook_url:
            self.mode = 'webhook'
            logger.info("飞书推送模式: Webhook")
        elif self.app_id and self.app_secret and self.chat_id:
            self.mode = 'app'
            logger.info("飞书推送模式: 应用")
        else:
            self.mode = 'none'
            logger.warning("飞书配置不完整，推送功能将无法使用")
    
    def _get_access_token(self) -> Optional[str]:
        """
        获取飞书访问令牌（仅应用模式需要）
        
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
        发送文件消息到飞书群（应用模式）
        
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
    
    def _send_via_webhook(self, message: Dict[str, Any]) -> bool:
        """
        通过 Webhook 发送消息
        
        Args:
            message: 消息内容
            
        Returns:
            是否发送成功
        """
        if not self.webhook_url:
            logger.error("Webhook URL 未配置")
            return False
        
        try:
            headers = {
                'Content-Type': 'application/json'
            }
            
            response = requests.post(
                self.webhook_url,
                headers=headers,
                json=message,
                timeout=10
            )
            response.raise_for_status()
            
            result = response.json()
            if result.get('StatusCode') == 0 or result.get('code') == 0:
                logger.info("Webhook 消息发送成功")
                return True
            else:
                logger.error(f"Webhook 消息发送失败: {result}")
                return False
                
        except Exception as e:
            logger.error(f"Webhook 消息发送异常: {e}")
            return False
    
    def _build_analysis_card(self, title: str, analysis_result: Dict[str, Any], 
                              html_path: Optional[str] = None) -> Dict[str, Any]:
        """
        构建分析结果卡片消息
        
        Args:
            title: 消息标题
            analysis_result: 分析结果
            html_path: HTML报告路径（可选）
            
        Returns:
            卡片消息内容
        """
        trend_analysis = analysis_result.get('trend_analysis', {})
        stock_analysis = analysis_result.get('stock_analysis', {})
        
        new_hot_topics = trend_analysis.get('new_hot_topics', [])
        rising_topics = trend_analysis.get('rising_topics', [])
        stock_opportunities = stock_analysis.get('stock_opportunities', [])
        market_sentiment = stock_analysis.get('market_sentiment', {})
        
        elements = []
        
        elements.append({
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**📊 {title}**\n生成时间: {analysis_result.get('timestamp', '未知')}"
            }
        })
        
        elements.append({"tag": "hr"})
        
        if new_hot_topics:
            topics_text = "\n".join([
                f"• **{t.get('title', '未知')}**\n  {t.get('reason', '')}"
                for t in new_hot_topics[:5]
            ])
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**🆕 新增热搜**\n{topics_text}"
                }
            })
            elements.append({"tag": "hr"})
        
        if rising_topics:
            rising_text = "\n".join([
                f"• **{t.get('title', '未知')}**\n  {t.get('trend', '')}\n  潜在影响: {t.get('potential_impact', '')}"
                for t in rising_topics[:5]
            ])
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**📈 热度上升话题**\n{rising_text}"
                }
            })
            elements.append({"tag": "hr"})
        
        if stock_opportunities:
            stocks_text = ""
            for opp in stock_opportunities[:3]:
                event = opp.get('event', '未知事件')
                impact = opp.get('impact_level', '中')
                industries = ", ".join(opp.get('related_industries', []))
                
                stocks = opp.get('related_stocks', [])
                stocks_info = "\n".join([
                    f"  - {s.get('stock_name', '')}({s.get('stock_code', '')}) "
                    f"[{s.get('signal_type', '')} 信心:{s.get('confidence', 0)}%]"
                    for s in stocks[:3]
                ])
                
                stocks_text += f"**🎯 {event}**\n"
                stocks_text += f"影响程度: {impact} | 关联行业: {industries}\n"
                if stocks_info:
                    stocks_text += f"相关股票:\n{stocks_info}\n"
                stocks_text += f"分析: {opp.get('analysis', '')[:100]}...\n\n"
            
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": f"**💼 LLM挖掘的潜在题材与股票建议**\n{stocks_text}"
                }
            })
            elements.append({"tag": "hr"})
        
        if market_sentiment:
            sentiment = market_sentiment.get('overall_sentiment', '谨慎')
            sentiment_reason = market_sentiment.get('sentiment_reason', '')
            hot_industries = ", ".join(market_sentiment.get('hot_industries', []))
            
            elements.append({
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": (
                        f"**📊 市场情绪**\n"
                        f"整体情绪: {sentiment}\n"
                        f"热门行业: {hot_industries}\n"
                        f"判断理由: {sentiment_reason}"
                    )
                }
            })
        
        elements.append({"tag": "hr"})
        elements.append({
            "tag": "note",
            "elements": [
                {
                    "tag": "plain_text",
                    "content": "⚠️ 免责声明：本报告仅供参考，不构成任何投资建议。股市有风险，投资需谨慎。"
                }
            ]
        })
        
        card = {
            "msg_type": "interactive",
            "card": {
                "config": {
                    "wide_screen_mode": True
                },
                "header": {
                    "title": {
                        "tag": "plain_text",
                        "content": title
                    },
                    "template": "blue"
                },
                "elements": elements
            }
        }
        
        return card
    
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
        
        if self.mode == 'none':
            logger.warning("飞书配置不完整，无法推送")
            return False
        
        if file_path and self.mode == 'app':
            return self.push_file(file_path, title)
        
        message = {
            "msg_type": "text",
            "content": {
                "text": f"{title}\n\n{content}"
            }
        }
        
        return self._send_via_webhook(message)
    
    def push_file(self, file_path: str, title: Optional[str] = None) -> bool:
        """
        推送文件（仅应用模式支持）
        
        Args:
            file_path: 文件路径
            title: 文件标题（可选）
            
        Returns:
            是否推送成功
        """
        if not self.enabled:
            logger.info("飞书推送已禁用，跳过")
            return False
        
        if self.mode != 'app':
            logger.warning("Webhook 模式不支持文件推送，请使用应用模式")
            return False
        
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
        推送分析卡片消息
        
        Args:
            title: 消息标题
            analysis_result: 分析结果
            html_path: HTML报告路径（可选）
            
        Returns:
            是否推送成功
        """
        if not self.enabled:
            logger.info("飞书推送已禁用，跳过")
            return False
        
        if self.mode == 'none':
            logger.warning("飞书配置不完整，无法推送")
            return False
        
        card = self._build_analysis_card(title, analysis_result, html_path)
        
        success = self._send_via_webhook(card)
        
        if success and html_path and self.mode == 'app':
            logger.info("尝试推送HTML报告文件...")
            file_success = self.push_file(html_path, f"HTML报告: {title}")
            if not file_success:
                logger.warning("HTML报告文件推送失败，但卡片消息已发送")
        
        return success

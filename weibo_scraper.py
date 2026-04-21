#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
微博热搜数据采集模块
"""

import json
import os
import re
from datetime import datetime
import requests
from bs4 import BeautifulSoup


class WeiboScraper:
    def __init__(self, config):
        self.config = config
        self.weibo_config = config.get('weibo', {})
        self.data_config = config.get('data', {})
        self.api_url = self.weibo_config.get('api_url', 'https://s.weibo.com/top/summary?cate=realtimehot')
        self.cookie_sub = self.weibo_config.get('cookie_sub', '')
        self.storage_dir = self.data_config.get('storage_dir', './data')
        
        # 确保数据存储目录存在
        os.makedirs(self.storage_dir, exist_ok=True)
    
    def _get_headers(self):
        """构建请求头"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Referer': 'https://weibo.com/',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
        # 如果有cookie_sub，添加到Cookie头
        if self.cookie_sub:
            headers['Cookie'] = f'SUB={self.cookie_sub}'
        
        return headers
    
    def fetch_hot_search(self):
        """获取微博热搜数据"""
        headers = self._get_headers()
        
        try:
            response = requests.get(self.api_url, headers=headers, timeout=30)
            response.raise_for_status()
            response.encoding = 'utf-8'
            
            # 解析HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 查找热搜列表
            hot_list = []
            
            # 微博热搜页面的结构
            # 通常包含在 <div class="data"> 或 <ul class="sina-hot"> 等标签中
            # 需要根据实际页面结构调整
            
            # 尝试多种可能的选择器
            selectors = [
                'div.data ul li',
                'ul.sina-hot li',
                'ul#pl_top_realtimehot li',
                'table tbody tr',
                'div.content ul li'
            ]
            
            items = []
            for selector in selectors:
                items = soup.select(selector)
                if items:
                    break
            
            # 如果找不到，尝试更通用的方法
            if not items:
                # 查找包含热搜内容的a标签
                links = soup.find_all('a', href=True)
                for link in links:
                    href = link.get('href', '')
                    if 'weibo.com' in href or 's.weibo.com' in href:
                        text = link.get_text(strip=True)
                        if text and len(text) > 1:
                            # 检查是否是热搜项
                            parent = link.parent
                            if parent:
                                rank_text = parent.get_text(strip=True)
                                # 尝试提取排名
                                rank_match = re.search(r'^(\d+)', rank_text)
                                if rank_match:
                                    rank = int(rank_match.group(1))
                                else:
                                    rank = 0
                                
                                # 尝试提取热度
                                hot_match = re.search(r'(\d+(?:\.\d+)?\s*[万]?)', rank_text)
                                if hot_match:
                                    hot = hot_match.group(1)
                                else:
                                    hot = '0'
                                
                                hot_list.append({
                                    'rank': rank,
                                    'title': text,
                                    'hot': hot,
                                    'url': href,
                                    'is_market': False
                                })
            else:
                # 处理找到的列表项
                for idx, item in enumerate(items):
                    # 跳过标题行
                    if idx == 0:
                        continue
                    
                    # 提取排名
                    rank_elem = item.select_one('td.td-01, .rank, .num')
                    if rank_elem:
                        rank_text = rank_elem.get_text(strip=True)
                        if rank_text.isdigit():
                            rank = int(rank_text)
                        else:
                            rank = idx
                    else:
                        rank = idx
                    
                    # 提取标题和链接
                    title_elem = item.select_one('td.td-02 a, .title a, .content a')
                    if title_elem:
                        title = title_elem.get_text(strip=True)
                        url = title_elem.get('href', '')
                        if not url.startswith('http'):
                            url = 'https://s.weibo.com' + url
                    else:
                        # 尝试直接从item中提取
                        links = item.find_all('a', href=True)
                        if links:
                            title = links[0].get_text(strip=True)
                            url = links[0].get('href', '')
                            if not url.startswith('http'):
                                url = 'https://s.weibo.com' + url
                        else:
                            title = ''
                            url = ''
                    
                    # 提取热度
                    hot_elem = item.select_one('td.td-02 span, .hot, .heat')
                    if hot_elem:
                        hot = hot_elem.get_text(strip=True)
                    else:
                        # 尝试从文本中提取
                        item_text = item.get_text(strip=True)
                        hot_match = re.search(r'(\d+(?:\.\d+)?\s*[万]?)', item_text)
                        if hot_match:
                            hot = hot_match.group(1)
                        else:
                            hot = '0'
                    
                    # 检查是否是置顶或特殊标记
                    is_market = False
                    market_elem = item.select_one('.icon-top, .icon-mark, .icon-hot')
                    if market_elem:
                        is_market = True
                    
                    # 跳过空标题
                    if title:
                        hot_list.append({
                            'rank': rank,
                            'title': title,
                            'hot': hot,
                            'url': url,
                            'is_market': is_market
                        })
            
            # 按排名排序
            hot_list.sort(key=lambda x: x['rank'])
            
            return {
                'timestamp': datetime.now().isoformat(),
                'total_count': len(hot_list),
                'hot_list': hot_list
            }
            
        except Exception as e:
            print(f"获取微博热搜数据失败: {e}")
            return None
    
    def save_to_json(self, data, filename=None):
        """保存数据到JSON文件"""
        if not data:
            print("没有数据可保存")
            return None
        
        if not filename:
            # 使用时间戳作为文件名
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'weibo_hot_{timestamp}.json'
        
        filepath = os.path.join(self.storage_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"数据已保存到: {filepath}")
            return filepath
        except Exception as e:
            print(f"保存数据失败: {e}")
            return None
    
    def get_history_data(self, days=None):
        """获取历史数据"""
        if days is None:
            days = self.data_config.get('history_days', 7)
        
        history_files = []
        
        # 获取所有JSON文件
        if os.path.exists(self.storage_dir):
            for filename in os.listdir(self.storage_dir):
                if filename.startswith('weibo_hot_') and filename.endswith('.json'):
                    filepath = os.path.join(self.storage_dir, filename)
                    history_files.append(filepath)
        
        # 按修改时间排序，取最近的days个
        history_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
        history_files = history_files[:days]
        
        history_data = []
        for filepath in history_files:
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    history_data.append(data)
            except Exception as e:
                print(f"读取历史数据失败 {filepath}: {e}")
        
        return history_data
    
    def run(self):
        """执行采集流程"""
        print("开始采集微博热搜数据...")
        
        # 获取热搜数据
        data = self.fetch_hot_search()
        
        if data:
            print(f"成功获取 {data['total_count']} 条热搜数据")
            
            # 保存数据
            filepath = self.save_to_json(data)
            
            if filepath:
                print("数据采集完成！")
                return data
            else:
                print("数据保存失败")
                return None
        else:
            print("数据采集失败")
            return None


if __name__ == '__main__':
    # 测试代码
    import configparser
    
    # 读取配置
    with open('config.json', 'r', encoding='utf-8') as f:
        config = json.load(f)
    
    # 创建采集器
    scraper = WeiboScraper(config)
    
    # 运行采集
    data = scraper.run()
    
    if data:
        print(f"\n采集的热搜数据示例:")
        for item in data['hot_list'][:5]:
            print(f"排名 {item['rank']}: {item['title']} (热度: {item['hot']})")

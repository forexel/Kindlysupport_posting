import { useEffect, useState } from 'react';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { Card } from '../components/ui/card';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../components/ui/tabs';
import { Separator } from '../components/ui/separator';
import { Save, CheckCircle2, AlertCircle, User, Cpu, Send as SendIcon } from 'lucide-react';
import { toast } from 'sonner';
import { api } from '../lib/api';

export function SettingsPage() {
  const [saving, setSaving] = useState(false);
  const [email, setEmail] = useState(localStorage.getItem('userEmail') || '');

  const [apiKey, setApiKey] = useState('');
  const [textModel, setTextModel] = useState('openai/gpt-4o-mini');
  const [visionModel, setVisionModel] = useState('meta-llama/llama-4-scout');
  const [imageModel, setImageModel] = useState('openai/gpt-5-image-mini');

  const [botToken, setBotToken] = useState('');
  const [adminUserId, setAdminUserId] = useState('');
  const [previewChatId, setPreviewChatId] = useState('');
  const [publishChatId, setPublishChatId] = useState('');
  const [webhookUrl, setWebhookUrl] = useState('');
  const [webhookSecret, setWebhookSecret] = useState('');

  const [vkEnabled, setVkEnabled] = useState(false);
  const [vkAccessToken, setVkAccessToken] = useState('');
  const [vkGroupId, setVkGroupId] = useState('');

  const [igEnabled, setIgEnabled] = useState(false);
  const [igAccessToken, setIgAccessToken] = useState('');
  const [igUserId, setIgUserId] = useState('');

  const [pinEnabled, setPinEnabled] = useState(false);
  const [pinAccessToken, setPinAccessToken] = useState('');
  const [pinBoardId, setPinBoardId] = useState('');

  const load = async () => {
    try {
      const s = await api<any>('/api/settings');
      setApiKey(s.openrouter_api_key || '');
      setTextModel(s.openrouter_text_model || textModel);
      setVisionModel(s.openrouter_vision_model || visionModel);
      setImageModel(s.openrouter_image_model || imageModel);
      setBotToken(s.telegram_bot_token || '');
      setAdminUserId(String(s.telegram_admin_user_id || ''));
      setPreviewChatId(s.telegram_preview_chat || '');
      setPublishChatId(s.telegram_publish_chat || '');
      setWebhookSecret(s.telegram_webhook_secret || '');
      setVkEnabled(Boolean(s.enable_vk));
      setVkAccessToken(s.vk_access_token || '');
      setVkGroupId(s.vk_group_id || '');
      setIgEnabled(Boolean(s.enable_instagram));
      setIgAccessToken(s.instagram_access_token || '');
      setIgUserId(s.instagram_ig_user_id || '');
      setPinEnabled(Boolean(s.enable_pinterest));
      setPinAccessToken(s.pinterest_access_token || '');
      setPinBoardId(s.pinterest_board_id || '');
    } catch (e: any) {
      toast.error(String(e?.message || e));
    }
  };

  useEffect(() => { load(); }, []);

  const handleSave = async () => {
    setSaving(true);
    try {
      const payload: Record<string, any> = {
        openrouter_text_model: textModel,
        openrouter_vision_model: visionModel,
        openrouter_image_model: imageModel,
        telegram_admin_user_id: adminUserId,
        telegram_preview_chat: previewChatId,
        telegram_publish_chat: publishChatId,
        enable_vk: vkEnabled,
        vk_group_id: vkGroupId,
        enable_instagram: igEnabled,
        instagram_ig_user_id: igUserId,
        enable_pinterest: pinEnabled,
        pinterest_board_id: pinBoardId,
      };
      if (apiKey && apiKey !== '***') payload.openrouter_api_key = apiKey;
      if (botToken && botToken !== '***') payload.telegram_bot_token = botToken;
      if (webhookSecret && webhookSecret !== '***') payload.telegram_webhook_secret = webhookSecret;
      if (vkAccessToken && vkAccessToken !== '***') payload.vk_access_token = vkAccessToken;
      if (igAccessToken && igAccessToken !== '***') payload.instagram_access_token = igAccessToken;
      if (pinAccessToken && pinAccessToken !== '***') payload.pinterest_access_token = pinAccessToken;

      await api('/api/settings', 'PUT', payload);
      toast.success('Настройки сохранены');
      await load();
    } catch (e: any) {
      toast.error(String(e?.message || e));
    } finally {
      setSaving(false);
    }
  };

  const handleTestConnection = async (service: string) => {
    try {
      const ready = await api<any>('/api/integrations/readiness');
      toast.success(`${service}: проверка выполнена`);
      if (ready.missing_required?.length) toast.info(`Не хватает: ${ready.missing_required.join(', ')}`);
    } catch (e: any) {
      toast.error(String(e?.message || e));
    }
  };

  const handleSetWebhook = async () => {
    if (!webhookUrl) return toast.error('Укажи публичный URL');
    try {
      await api('/api/telegram/set-webhook', 'POST', { public_url: webhookUrl });
      toast.success('Webhook установлен');
    } catch (e: any) {
      toast.error(String(e?.message || e));
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-2 text-sm text-zinc-400"><span>Главная</span><span>/</span><span className="text-zinc-200">Настройки</span></div>

      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div><h1 className="text-3xl font-bold text-zinc-50">Настройки</h1><p className="text-zinc-400 mt-1">Управление аккаунтом и интеграциями</p></div>
        <Button onClick={handleSave} disabled={saving} className="bg-blue-600 hover:bg-blue-700 text-white">{saving ? <><AlertCircle className="mr-2 h-4 w-4 animate-spin" />Сохранение...</> : <><Save className="mr-2 h-4 w-4" />Сохранить всё</>}</Button>
      </div>

      <Tabs defaultValue="profile" className="w-full">
        <TabsList className="bg-zinc-900 border border-zinc-800 grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-7">
          <TabsTrigger value="profile" className="data-[state=active]:bg-zinc-800 data-[state=active]:text-zinc-100"><User className="h-4 w-4 sm:mr-2" /><span className="hidden sm:inline">Профиль</span></TabsTrigger>
          <TabsTrigger value="llm" className="data-[state=active]:bg-zinc-800 data-[state=active]:text-zinc-100"><Cpu className="h-4 w-4 sm:mr-2" /><span className="hidden sm:inline">LLM</span></TabsTrigger>
          <TabsTrigger value="telegram" className="data-[state=active]:bg-zinc-800 data-[state=active]:text-zinc-100"><SendIcon className="h-4 w-4 sm:mr-2" /><span className="hidden sm:inline">Telegram</span></TabsTrigger>
          <TabsTrigger value="vk" className="data-[state=active]:bg-zinc-800 data-[state=active]:text-zinc-100">VK</TabsTrigger>
          <TabsTrigger value="instagram" className="data-[state=active]:bg-zinc-800 data-[state=active]:text-zinc-100">Instagram</TabsTrigger>
          <TabsTrigger value="pinterest" className="data-[state=active]:bg-zinc-800 data-[state=active]:text-zinc-100">Pinterest</TabsTrigger>
          <TabsTrigger value="actions" className="data-[state=active]:bg-zinc-800 data-[state=active]:text-zinc-100">Действия</TabsTrigger>
        </TabsList>

        <TabsContent value="profile" className="mt-6">
          <Card className="bg-zinc-900 border-zinc-800 p-6 space-y-4">
            <h2 className="text-xl font-semibold text-zinc-50">Профиль аккаунта</h2>
            <div className="space-y-2"><Label htmlFor="email" className="text-zinc-200">Email</Label><Input id="email" type="email" value={email} onChange={(e) => setEmail(e.target.value)} className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
          </Card>
        </TabsContent>

        <TabsContent value="llm" className="mt-6">
          <Card className="bg-zinc-900 border-zinc-800 p-6 space-y-4">
            <h2 className="text-xl font-semibold text-zinc-50">LLM / OpenRouter</h2>
            <div className="space-y-2"><Label className="text-zinc-200">API Key</Label><Input type="password" value={apiKey} onChange={(e) => setApiKey(e.target.value)} placeholder="sk-or-..." className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="space-y-2"><Label className="text-zinc-200">Text Model</Label><Input value={textModel} onChange={(e) => setTextModel(e.target.value)} className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
              <div className="space-y-2"><Label className="text-zinc-200">Vision Model</Label><Input value={visionModel} onChange={(e) => setVisionModel(e.target.value)} className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
              <div className="space-y-2"><Label className="text-zinc-200">Image Model</Label><Input value={imageModel} onChange={(e) => setImageModel(e.target.value)} className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
            </div>
            <Button onClick={() => handleTestConnection('OpenRouter')} variant="outline" className="bg-zinc-800 border-zinc-700 text-zinc-300 hover:bg-zinc-700"><CheckCircle2 className="mr-2 h-4 w-4" />Проверить подключение</Button>
          </Card>
        </TabsContent>

        <TabsContent value="telegram" className="mt-6">
          <Card className="bg-zinc-900 border-zinc-800 p-6 space-y-4">
            <h2 className="text-xl font-semibold text-zinc-50">Telegram</h2>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="space-y-2"><Label className="text-zinc-200">Bot Token</Label><Input type="password" value={botToken} onChange={(e) => setBotToken(e.target.value)} className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
              <div className="space-y-2"><Label className="text-zinc-200">Admin User ID</Label><Input value={adminUserId} onChange={(e) => setAdminUserId(e.target.value)} className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
            </div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="space-y-2"><Label className="text-zinc-200">Preview Chat ID</Label><Input value={previewChatId} onChange={(e) => setPreviewChatId(e.target.value)} className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
              <div className="space-y-2"><Label className="text-zinc-200">Publish Chat ID</Label><Input value={publishChatId} onChange={(e) => setPublishChatId(e.target.value)} className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
            </div>
            <Separator className="bg-zinc-800" />
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="space-y-2"><Label className="text-zinc-200">Public URL (для setWebhook)</Label><Input value={webhookUrl} onChange={(e) => setWebhookUrl(e.target.value)} placeholder="https://posting.testmakerapp.online" className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
              <div className="space-y-2"><Label className="text-zinc-200">Webhook Secret</Label><Input type="password" value={webhookSecret} onChange={(e) => setWebhookSecret(e.target.value)} className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
            </div>
            <div className="flex gap-2">
              <Button onClick={() => handleTestConnection('Telegram')} variant="outline" className="bg-zinc-800 border-zinc-700 text-zinc-300 hover:bg-zinc-700"><CheckCircle2 className="mr-2 h-4 w-4" />Проверить подключение</Button>
              <Button onClick={handleSetWebhook} variant="outline" className="bg-zinc-800 border-zinc-700 text-zinc-300 hover:bg-zinc-700">Установить webhook</Button>
            </div>
          </Card>
        </TabsContent>

        <TabsContent value="vk" className="mt-6">
          <Card className="bg-zinc-900 border-zinc-800 p-6 space-y-4">
            <h2 className="text-xl font-semibold text-zinc-50">VK</h2>
            <div className="flex items-center gap-2"><input type="checkbox" checked={vkEnabled} onChange={(e) => setVkEnabled(e.target.checked)} /><span className="text-zinc-300">Включить VK</span></div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="space-y-2"><Label className="text-zinc-200">Access Token</Label><Input type="password" value={vkAccessToken} onChange={(e) => setVkAccessToken(e.target.value)} className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
              <div className="space-y-2"><Label className="text-zinc-200">Group ID</Label><Input value={vkGroupId} onChange={(e) => setVkGroupId(e.target.value)} className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
            </div>
          </Card>
        </TabsContent>

        <TabsContent value="instagram" className="mt-6">
          <Card className="bg-zinc-900 border-zinc-800 p-6 space-y-4">
            <h2 className="text-xl font-semibold text-zinc-50">Instagram</h2>
            <div className="flex items-center gap-2"><input type="checkbox" checked={igEnabled} onChange={(e) => setIgEnabled(e.target.checked)} /><span className="text-zinc-300">Включить Instagram</span></div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="space-y-2"><Label className="text-zinc-200">Access Token</Label><Input type="password" value={igAccessToken} onChange={(e) => setIgAccessToken(e.target.value)} className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
              <div className="space-y-2"><Label className="text-zinc-200">IG User ID</Label><Input value={igUserId} onChange={(e) => setIgUserId(e.target.value)} className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
            </div>
          </Card>
        </TabsContent>

        <TabsContent value="pinterest" className="mt-6">
          <Card className="bg-zinc-900 border-zinc-800 p-6 space-y-4">
            <h2 className="text-xl font-semibold text-zinc-50">Pinterest</h2>
            <div className="flex items-center gap-2"><input type="checkbox" checked={pinEnabled} onChange={(e) => setPinEnabled(e.target.checked)} /><span className="text-zinc-300">Включить Pinterest</span></div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="space-y-2"><Label className="text-zinc-200">Access Token</Label><Input type="password" value={pinAccessToken} onChange={(e) => setPinAccessToken(e.target.value)} className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
              <div className="space-y-2"><Label className="text-zinc-200">Board ID</Label><Input value={pinBoardId} onChange={(e) => setPinBoardId(e.target.value)} className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
            </div>
          </Card>
        </TabsContent>

        <TabsContent value="actions" className="mt-6">
          <Card className="bg-zinc-900 border-zinc-800 p-6 space-y-4">
            <h2 className="text-xl font-semibold text-zinc-50">Проверки</h2>
            <div className="flex gap-2">
              <Button onClick={() => handleTestConnection('Интеграции')} variant="outline" className="bg-zinc-800 border-zinc-700 text-zinc-300 hover:bg-zinc-700"><CheckCircle2 className="mr-2 h-4 w-4" />Проверить все интеграции</Button>
            </div>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}

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
  const [textModel, setTextModel] = useState('google/gemini-2.5-flash-lite');
  const [visionModel, setVisionModel] = useState('meta-llama/llama-4-scout');
  const [imageModel, setImageModel] = useState('black-forest-labs/flux.2-pro');
  const [modelsLocked, setModelsLocked] = useState(true);

  const [botToken, setBotToken] = useState('');
  const [adminUserId, setAdminUserId] = useState('');
  const [previewChatId, setPreviewChatId] = useState('');
  const [publishChatId, setPublishChatId] = useState('');
  const [webhookUrl, setWebhookUrl] = useState('');
  const [webhookSecret, setWebhookSecret] = useState('');

  const [vkEnabled, setVkEnabled] = useState(false);
  const [vkAccessToken, setVkAccessToken] = useState('');
  const [vkGroupId, setVkGroupId] = useState('');
  const [maxEnabled, setMaxEnabled] = useState(false);
  const [maxPublishUrl, setMaxPublishUrl] = useState('');
  const [maxAccessToken, setMaxAccessToken] = useState('');
  const [maxHttpHeader, setMaxHttpHeader] = useState('Authorization');

  const [igEnabled, setIgEnabled] = useState(false);
  const [igDeliveryMode, setIgDeliveryMode] = useState('external_queue');
  const [igAccessToken, setIgAccessToken] = useState('');
  const [igUserId, setIgUserId] = useState('');
  const [igQueueGithubToken, setIgQueueGithubToken] = useState('');
  const [igQueueRepo, setIgQueueRepo] = useState('');
  const [igQueueBranch, setIgQueueBranch] = useState('main');
  const [igQueuePath, setIgQueuePath] = useState('queue/instagram');

  const [pinEnabled, setPinEnabled] = useState(false);
  const [pinAccessToken, setPinAccessToken] = useState('');
  const [pinBoardId, setPinBoardId] = useState('');

  const applyModelPreset = () => {
    setTextModel('google/gemini-2.5-flash-lite');
    setVisionModel('meta-llama/llama-4-scout');
    setImageModel('black-forest-labs/flux.2-pro');
  };

  const load = async () => {
    try {
      const s = await api<any>('/api/settings');
      setModelsLocked(Boolean(s.models_locked));
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
      setMaxEnabled(Boolean(s.enable_max));
      setMaxPublishUrl(s.max_publish_url || '');
      setMaxAccessToken(s.max_access_token || '');
      setMaxHttpHeader(s.max_http_header || 'Authorization');
      setIgEnabled(Boolean(s.enable_instagram));
      setIgDeliveryMode(s.instagram_delivery_mode || 'external_queue');
      setIgAccessToken(s.instagram_access_token || '');
      setIgUserId(s.instagram_ig_user_id || '');
      setIgQueueGithubToken(s.instagram_queue_github_token || '');
      setIgQueueRepo(s.instagram_queue_repo || '');
      setIgQueueBranch(s.instagram_queue_branch || 'main');
      setIgQueuePath(s.instagram_queue_path || 'queue/instagram');
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
        enable_max: maxEnabled,
        max_publish_url: maxPublishUrl,
        max_http_header: maxHttpHeader,
        enable_instagram: igEnabled,
        instagram_delivery_mode: igDeliveryMode,
        instagram_ig_user_id: igUserId,
        instagram_queue_repo: igQueueRepo,
        instagram_queue_branch: igQueueBranch,
        instagram_queue_path: igQueuePath,
        enable_pinterest: pinEnabled,
        pinterest_board_id: pinBoardId,
      };
      if (apiKey && apiKey !== '***') payload.openrouter_api_key = apiKey;
      if (botToken && botToken !== '***') payload.telegram_bot_token = botToken;
      if (webhookSecret && webhookSecret !== '***') payload.telegram_webhook_secret = webhookSecret;
      if (vkAccessToken && vkAccessToken !== '***') payload.vk_access_token = vkAccessToken;
      if (maxAccessToken && maxAccessToken !== '***') payload.max_access_token = maxAccessToken;
      if (igAccessToken && igAccessToken !== '***') payload.instagram_access_token = igAccessToken;
      if (igQueueGithubToken && igQueueGithubToken !== '***') payload.instagram_queue_github_token = igQueueGithubToken;
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
        <TabsList className="bg-zinc-900 border border-zinc-800 grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-8">
          <TabsTrigger value="profile" className="data-[state=active]:bg-zinc-800 data-[state=active]:text-zinc-100"><User className="h-4 w-4 sm:mr-2" /><span className="hidden sm:inline">Профиль</span></TabsTrigger>
          <TabsTrigger value="llm" className="data-[state=active]:bg-zinc-800 data-[state=active]:text-zinc-100"><Cpu className="h-4 w-4 sm:mr-2" /><span className="hidden sm:inline">LLM</span></TabsTrigger>
          <TabsTrigger value="telegram" className="data-[state=active]:bg-zinc-800 data-[state=active]:text-zinc-100"><SendIcon className="h-4 w-4 sm:mr-2" /><span className="hidden sm:inline">Telegram</span></TabsTrigger>
          <TabsTrigger value="vk" className="data-[state=active]:bg-zinc-800 data-[state=active]:text-zinc-100">VK</TabsTrigger>
          <TabsTrigger value="max" className="data-[state=active]:bg-zinc-800 data-[state=active]:text-zinc-100">MAX</TabsTrigger>
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
            <div className="space-y-2">
              <Label className="text-zinc-200">Быстрые наборы моделей</Label>
              <div className="flex flex-wrap gap-2">
                <Button
                  type="button"
                  variant="outline"
                  className="bg-zinc-800 border-zinc-700 text-zinc-300 hover:bg-zinc-700"
                  onClick={applyModelPreset}
                >
                  Gemini 2.5 Flash Lite + Llama Scout + Flux.2-pro
                </Button>
              </div>
            </div>
            {modelsLocked && (
              <p className="text-xs text-amber-400">
                Модели зафиксированы на сервере: Text=Gemini 2.5 Flash Lite, Vision=Llama 4 Scout, Image=Flux.2-pro.
              </p>
            )}
            <div className="space-y-2"><Label className="text-zinc-200">API Key</Label><Input type="password" value={apiKey} onChange={(e) => setApiKey(e.target.value)} placeholder="sk-or-..." className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="space-y-2"><Label className="text-zinc-200">Text Model</Label><Input list="text-models" value={textModel} onChange={(e) => setTextModel(e.target.value)} disabled={modelsLocked} className="bg-zinc-800 border-zinc-700 text-zinc-100 disabled:opacity-60" /></div>
              <div className="space-y-2"><Label className="text-zinc-200">Vision Model</Label><Input list="vision-models" value={visionModel} onChange={(e) => setVisionModel(e.target.value)} disabled={modelsLocked} className="bg-zinc-800 border-zinc-700 text-zinc-100 disabled:opacity-60" /></div>
              <div className="space-y-2"><Label className="text-zinc-200">Image Model</Label><Input list="image-models" value={imageModel} onChange={(e) => setImageModel(e.target.value)} disabled={modelsLocked} className="bg-zinc-800 border-zinc-700 text-zinc-100 disabled:opacity-60" /></div>
            </div>
            <datalist id="text-models">
              <option value="google/gemini-2.5-flash-lite" />
              <option value="google/gemini-2.5-pro" />
            </datalist>
            <datalist id="vision-models">
              <option value="meta-llama/llama-4-scout" />
            </datalist>
            <datalist id="image-models">
              <option value="black-forest-labs/flux.2-pro" />
            </datalist>
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

        <TabsContent value="max" className="mt-6">
          <Card className="bg-zinc-900 border-zinc-800 p-6 space-y-4">
            <h2 className="text-xl font-semibold text-zinc-50">MAX</h2>
            <div className="flex items-center gap-2"><input type="checkbox" checked={maxEnabled} onChange={(e) => setMaxEnabled(e.target.checked)} /><span className="text-zinc-300">Включить MAX</span></div>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="space-y-2"><Label className="text-zinc-200">Publish URL</Label><Input value={maxPublishUrl} onChange={(e) => setMaxPublishUrl(e.target.value)} placeholder="https://.../publish" className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
              <div className="space-y-2"><Label className="text-zinc-200">HTTP Header</Label><Input value={maxHttpHeader} onChange={(e) => setMaxHttpHeader(e.target.value)} placeholder="Authorization" className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
            </div>
            <div className="space-y-2"><Label className="text-zinc-200">Access Token (optional)</Label><Input type="password" value={maxAccessToken} onChange={(e) => setMaxAccessToken(e.target.value)} className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
          </Card>
        </TabsContent>

        <TabsContent value="instagram" className="mt-6">
          <Card className="bg-zinc-900 border-zinc-800 p-6 space-y-4">
            <h2 className="text-xl font-semibold text-zinc-50">Instagram</h2>
            <div className="flex items-center gap-2"><input type="checkbox" checked={igEnabled} onChange={(e) => setIgEnabled(e.target.checked)} /><span className="text-zinc-300">Включить Instagram</span></div>
            <div className="space-y-2">
              <Label className="text-zinc-200">Режим доставки</Label>
              <select value={igDeliveryMode} onChange={(e) => setIgDeliveryMode(e.target.value)} className="w-full h-10 rounded-md border border-zinc-700 bg-zinc-800 px-3 text-zinc-100">
                <option value="external_queue">Через внешнюю очередь GitHub</option>
                <option value="direct">Прямой запрос в Graph API</option>
              </select>
            </div>
            {igDeliveryMode === 'direct' ? (
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="space-y-2"><Label className="text-zinc-200">Access Token</Label><Input type="password" value={igAccessToken} onChange={(e) => setIgAccessToken(e.target.value)} className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
                <div className="space-y-2"><Label className="text-zinc-200">IG User ID</Label><Input value={igUserId} onChange={(e) => setIgUserId(e.target.value)} className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
              </div>
            ) : (
              <div className="space-y-4">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div className="space-y-2"><Label className="text-zinc-200">GitHub Repo (owner/repo)</Label><Input value={igQueueRepo} onChange={(e) => setIgQueueRepo(e.target.value)} placeholder="forexel/Kindlysupport_posting" className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
                  <div className="space-y-2"><Label className="text-zinc-200">GitHub Token</Label><Input type="password" value={igQueueGithubToken} onChange={(e) => setIgQueueGithubToken(e.target.value)} className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div className="space-y-2"><Label className="text-zinc-200">Branch</Label><Input value={igQueueBranch} onChange={(e) => setIgQueueBranch(e.target.value)} placeholder="main" className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
                  <div className="space-y-2"><Label className="text-zinc-200">Queue Path</Label><Input value={igQueuePath} onChange={(e) => setIgQueuePath(e.target.value)} placeholder="queue/instagram" className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
                </div>
              </div>
            )}
            <div className="text-xs text-zinc-400">
              В режиме очереди приложение только кладёт JSON-задачу в GitHub, публикацию делает workflow Instagram Publisher.
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

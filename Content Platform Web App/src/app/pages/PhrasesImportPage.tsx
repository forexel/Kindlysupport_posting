import { useState } from 'react';
import { Link } from 'react-router';
import { Button } from '../components/ui/button';
import { Textarea } from '../components/ui/textarea';
import { Label } from '../components/ui/label';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../components/ui/tabs';
import { RadioGroup, RadioGroupItem } from '../components/ui/radio-group';
import { Progress } from '../components/ui/progress';
import { Input } from '../components/ui/input';
import { Card } from '../components/ui/card';
import { Checkbox } from '../components/ui/checkbox';
import { ArrowLeft, Upload, FileText, Image, Loader2, Check, X, Edit2 } from 'lucide-react';
import { toast } from 'sonner';
import { api, toDataUrl } from '../lib/api';

interface OCRResult {
  id: string;
  text: string;
  fileName: string;
  status: 'processing' | 'completed' | 'error';
  editable: boolean;
  selected: boolean;
  error?: string;
}

export function PhrasesImportPage() {
  const [textInput, setTextInput] = useState('');
  const [defaultStatus, setDefaultStatus] = useState<'0' | '1'>('0');
  const [files, setFiles] = useState<OCRResult[]>([]);
  const [isDragging, setIsDragging] = useState(false);

  const handleTextImport = async () => {
    const lines = textInput.split('\n').filter((line) => line.trim());
    if (!lines.length) return;
    try {
      const res = await api<{ parsed: number; inserted: number; updated: number }>('/api/phrases/import-text', 'POST', {
        raw_text: lines.join('\n'),
        is_published: Number(defaultStatus),
      });
      toast.success(`Импорт: parsed=${res.parsed}, inserted=${res.inserted}, updated=${res.updated}`);
      setTextInput('');
    } catch (e: any) {
      toast.error(String(e?.message || e));
    }
  };

  const handleFileUpload = async (uploadedFiles: FileList | null) => {
    if (!uploadedFiles) return;
    const selected = Array.from(uploadedFiles).filter((f) => f.type.startsWith('image/'));
    if (!selected.length) return;

    const initial: OCRResult[] = selected.map((file) => ({
      id: Math.random().toString(36).slice(2),
      text: '',
      fileName: file.name,
      status: 'processing',
      editable: false,
      selected: false,
    }));
    setFiles((prev) => [...prev, ...initial]);

    try {
      const images = await Promise.all(selected.map(async (f) => ({ name: f.name, image_data_url: await toDataUrl(f) })));
      const res = await api<{ items: any[]; recognized: number; total: number }>('/api/phrases/ocr-images-base64', 'POST', { images });

      setFiles((prev) => {
        const initialIds = new Set(initial.map((x) => x.id));
        const keep = prev.filter((x) => !initialIds.has(x.id));
        const expanded: OCRResult[] = [];

        for (const base of initial) {
          const item = res.items.find((x) => x.name === base.fileName);
          if (!item || !item.ok) {
            expanded.push({
              ...base,
              status: 'error',
              error: (item && item.error) || 'OCR error',
              selected: false,
            });
            continue;
          }

          const phrases: string[] = Array.isArray(item.phrases)
            ? item.phrases.map((p: any) => String(p || '').trim()).filter(Boolean)
            : [String(item.phrase || '').trim()].filter(Boolean);

          if (!phrases.length) {
            expanded.push({
              ...base,
              status: 'error',
              error: 'Фразы не найдены',
              selected: false,
            });
            continue;
          }

          phrases.forEach((p, idx) => {
            expanded.push({
              id: `${base.id}-${idx + 1}`,
              fileName: phrases.length > 1 ? `${base.fileName} • ${idx + 1}/${phrases.length}` : base.fileName,
              status: 'completed',
              text: p,
              editable: false,
              selected: true,
            });
          });
        }

        return [...keep, ...expanded];
      });
      toast.success(`Распознано ${res.recognized} из ${res.total}`);
    } catch (e: any) {
      setFiles((prev) => prev.map((f) => (f.status === 'processing' ? { ...f, status: 'error', error: String(e?.message || e) } : f)));
      toast.error(String(e?.message || e));
    }
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
    handleFileUpload(e.dataTransfer.files);
  };

  const handleEditPhrase = (id: string, newText: string) => setFiles(files.map((f) => (f.id === id ? { ...f, text: newText } : f)));
  const handleToggleEdit = (id: string) => setFiles(files.map((f) => (f.id === id ? { ...f, editable: !f.editable } : f)));
  const handleRemoveFile = (id: string) => setFiles(files.filter((f) => f.id !== id));
  const handleToggleSelected = (id: string, checked: boolean) => setFiles(files.map((f) => (f.id === id ? { ...f, selected: checked } : f)));
  const handleSelectAllCompleted = (checked: boolean) =>
    setFiles(files.map((f) => (f.status === 'completed' && f.text.trim() ? { ...f, selected: checked } : f)));

  const handleAcceptAll = async () => {
    const phrases = files
      .filter((f) => f.status === 'completed' && f.selected && f.text.trim())
      .map((f) => f.text.trim());
    if (!phrases.length) return toast.error('Нет распознанных фраз');
    try {
      const res = await api<{ parsed: number; inserted: number; updated: number }>('/api/phrases/import-text', 'POST', {
        raw_text: phrases.join('\n'),
        is_published: Number(defaultStatus),
      });
      toast.success(`Добавлено: parsed=${res.parsed}, inserted=${res.inserted}, updated=${res.updated}`);
      setFiles([]);
    } catch (e: any) {
      toast.error(String(e?.message || e));
    }
  };

  const linesCount = textInput.split('\n').filter((line) => line.trim()).length;
  const processedCount = files.filter((f) => f.status === 'completed').length;
  const totalCount = files.length;
  const selectedCount = files.filter((f) => f.status === 'completed' && f.selected && f.text.trim()).length;

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-2 text-sm text-zinc-400"><Link to="/phrases" className="hover:text-zinc-200">Фразы</Link><span>/</span><span className="text-zinc-200">Импорт</span></div>

      <div className="flex items-center gap-4">
        <Link to="/phrases"><Button variant="ghost" size="sm" className="text-zinc-400 hover:text-zinc-200"><ArrowLeft className="h-4 w-4" /></Button></Link>
        <div><h1 className="text-3xl font-bold text-zinc-50">Импорт фраз</h1><p className="text-zinc-400 mt-1">Добавьте фразы через текст или распознавание изображений</p></div>
      </div>

      <Tabs defaultValue="text" className="w-full">
        <TabsList className="bg-zinc-900 border border-zinc-800">
          <TabsTrigger value="text" className="data-[state=active]:bg-zinc-800 data-[state=active]:text-zinc-100"><FileText className="mr-2 h-4 w-4" />Вставка текста</TabsTrigger>
          <TabsTrigger value="images" className="data-[state=active]:bg-zinc-800 data-[state=active]:text-zinc-100"><Image className="mr-2 h-4 w-4" />Загрузка изображений</TabsTrigger>
        </TabsList>

        <TabsContent value="text" className="space-y-4 mt-6">
          <Card className="bg-zinc-900 border-zinc-800 p-6 space-y-4">
            <div className="space-y-2">
              <Label htmlFor="phrases-text" className="text-zinc-200">Текст фраз (одна фраза = одна строка)</Label>
              <Textarea id="phrases-text" value={textInput} onChange={(e) => setTextInput(e.target.value)} placeholder="Введите фразы, каждая с новой строки..." rows={10} className="bg-zinc-800 border-zinc-700 text-zinc-100 placeholder:text-zinc-500 resize-none" />
              <p className="text-sm text-zinc-500">Обнаружено строк: {linesCount}</p>
            </div>

            <div className="space-y-3">
              <Label className="text-zinc-200">Статус по умолчанию</Label>
              <RadioGroup value={defaultStatus} onValueChange={(v) => setDefaultStatus(v as '0' | '1')}>
                <div className="flex items-center space-x-2"><RadioGroupItem value="0" id="status-new" className="border-zinc-700" /><Label htmlFor="status-new" className="text-zinc-300 cursor-pointer">Новая (0)</Label></div>
                <div className="flex items-center space-x-2"><RadioGroupItem value="1" id="status-published" className="border-zinc-700" /><Label htmlFor="status-published" className="text-zinc-300 cursor-pointer">Опубликованная (1)</Label></div>
              </RadioGroup>
            </div>

            <Button onClick={handleTextImport} disabled={linesCount === 0} className="w-full bg-blue-600 hover:bg-blue-700 text-white">Добавить {linesCount > 0 && `(${linesCount})`}</Button>
          </Card>
        </TabsContent>

        <TabsContent value="images" className="space-y-4 mt-6">
          <Card className="bg-zinc-900 border-zinc-800 p-6 space-y-4">
            <div
              onDrop={handleDrop}
              onDragOver={(e) => { e.preventDefault(); setIsDragging(true); }}
              onDragLeave={() => setIsDragging(false)}
              className={`border-2 border-dashed rounded-lg p-12 text-center transition-colors ${isDragging ? 'border-blue-500 bg-blue-950/20' : 'border-zinc-700 hover:border-zinc-600'}`}
            >
              <Upload className="h-12 w-12 mx-auto mb-4 text-zinc-500" />
              <p className="text-zinc-300 mb-2">Перетащите изображения сюда или</p>
              <label>
                <Input type="file" multiple accept="image/*" onChange={(e) => handleFileUpload(e.target.files)} className="hidden" />
                <Button variant="outline" className="bg-zinc-800 border-zinc-700 text-zinc-300" asChild><span>Выбрать файлы</span></Button>
              </label>
            </div>

            {files.length > 0 && (
              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <p className="text-sm text-zinc-300">Распознано {processedCount} из {totalCount} • Выбрано {selectedCount}</p>
                  <div className="flex gap-2">
                    <Button size="sm" variant="outline" onClick={() => handleSelectAllCompleted(true)} className="bg-zinc-800 border-zinc-700 text-zinc-300 hover:bg-zinc-700">Отметить все</Button>
                    <Button size="sm" variant="outline" onClick={() => handleSelectAllCompleted(false)} className="bg-zinc-800 border-zinc-700 text-zinc-300 hover:bg-zinc-700">Снять все</Button>
                    <Button size="sm" variant="outline" onClick={() => setFiles([])} className="bg-zinc-800 border-zinc-700 text-zinc-300 hover:bg-zinc-700">Отменить</Button>
                    <Button size="sm" onClick={handleAcceptAll} disabled={selectedCount === 0} className="bg-green-600 hover:bg-green-700 text-white"><Check className="mr-2 h-4 w-4" />Принять все</Button>
                  </div>
                </div>

                <Progress value={totalCount ? (processedCount / totalCount) * 100 : 0} className="h-2" />

                <div className="space-y-2 max-h-96 overflow-y-auto">
                  {files.map((file) => (
                    <div
                      key={file.id}
                      className={`rounded-lg p-4 space-y-2 border ${
                        file.selected ? 'bg-green-950/20 border-green-800' : 'bg-zinc-800 border-zinc-700'
                      }`}
                    >
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-3">
                          <Checkbox
                            checked={file.selected}
                            disabled={!(file.status === 'completed' && file.text.trim())}
                            onCheckedChange={(checked) => handleToggleSelected(file.id, Boolean(checked))}
                            className="border-zinc-500"
                          />
                          <span className="text-xs text-zinc-400 min-w-[64px]">Сохранить</span>
                          {file.status === 'processing' && <Loader2 className="h-4 w-4 text-blue-400 animate-spin" />}
                          {file.status === 'completed' && <Check className="h-4 w-4 text-green-400" />}
                          {file.status === 'error' && <X className="h-4 w-4 text-red-400" />}
                          <span className="text-sm text-zinc-400">{file.fileName}</span>
                        </div>
                        <div className="flex gap-2">
                          {file.status === 'completed' && <Button size="sm" variant="ghost" onClick={() => handleToggleEdit(file.id)} className="text-zinc-400 hover:text-zinc-200"><Edit2 className="h-4 w-4" /></Button>}
                          <Button size="sm" variant="ghost" onClick={() => handleRemoveFile(file.id)} className="text-red-400 hover:text-red-300"><X className="h-4 w-4" /></Button>
                        </div>
                      </div>
                      {file.status === 'completed' && (
                        file.editable ? <Input value={file.text} onChange={(e) => handleEditPhrase(file.id, e.target.value)} className="bg-zinc-900 border-zinc-600 text-zinc-100" /> : <p className="text-zinc-200">{file.text}</p>
                      )}
                      {file.status === 'processing' && <p className="text-sm text-zinc-500">Распознавание...</p>}
                      {file.status === 'error' && <p className="text-sm text-red-400">{file.error || 'Ошибка OCR'}</p>}
                    </div>
                  ))}
                </div>
              </div>
            )}
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}

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
  author?: string;
  ocrEngine?: string;
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
      const indexed = selected.map((file, idx) => ({ file, base: initial[idx] }));
      const images = await Promise.all(
        indexed.map(async ({ file, base }) => ({
          name: base.id,
          image_data_url: await toDataUrl(file, { maxSide: 1600, quality: 0.82 }),
        })),
      );

      const chunkSize = 4;
      const allItems: any[] = [];
      let recognized = 0;
      for (let i = 0; i < images.length; i += chunkSize) {
        const chunk = images.slice(i, i + chunkSize);
        const res = await api<{ items: any[]; recognized: number; total: number }>('/api/phrases/ocr-images-base64', 'POST', { images: chunk });
        allItems.push(...(res.items || []));
        recognized += Number(res.recognized || 0);
      }

      setFiles((prev) => {
        const initialIds = new Set(initial.map((x) => x.id));
        const keep = prev.filter((x) => !initialIds.has(x.id));
        const expanded: OCRResult[] = [];

        for (const base of initial) {
          const item = allItems.find((x) => x.name === base.id);
          if (!item || !item.ok) {
            expanded.push({
              ...base,
              status: 'error',
              error: (item && item.error) || 'OCR error',
              selected: false,
              ocrEngine: String((item && item.ocr_engine) || ''),
            });
            continue;
          }

          const structPhrases: Array<{ text_body: string; author?: string }> = Array.isArray(item.phrases_struct)
            ? item.phrases_struct
                .map((p: any) => ({
                  text_body: String((p && p.text_body) || '').trim(),
                  author: String((p && p.author) || '').trim(),
                }))
                .filter((p: any) => p.text_body)
            : [];
          const phrases: Array<{ text_body: string; author?: string }> = structPhrases.length
            ? structPhrases
            : (Array.isArray(item.phrases)
                ? item.phrases.map((p: any) => ({ text_body: String(p || '').trim(), author: '' })).filter((p: any) => p.text_body)
                : [{ text_body: String(item.phrase || '').trim(), author: '' }].filter((p: any) => p.text_body));

          if (!phrases.length) {
            expanded.push({
              ...base,
              status: 'error',
              error: 'Фразы не найдены',
              selected: false,
              ocrEngine: String(item.ocr_engine || ''),
            });
            continue;
          }

          phrases.forEach((p, idx) => {
            expanded.push({
              id: `${base.id}-${idx + 1}`,
              fileName: phrases.length > 1 ? `${base.fileName} • ${idx + 1}/${phrases.length}` : base.fileName,
              status: 'completed',
              text: p.text_body,
              author: p.author || '',
              editable: false,
              selected: true,
              ocrEngine: String(item.ocr_engine || ''),
            });
          });
        }

        return [...keep, ...expanded];
      });
      toast.success(`Распознано ${recognized} из ${images.length}`);
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
  const handleEditAuthor = (id: string, newAuthor: string) => setFiles(files.map((f) => (f.id === id ? { ...f, author: newAuthor } : f)));
  const handleToggleEdit = (id: string) => setFiles(files.map((f) => (f.id === id ? { ...f, editable: !f.editable } : f)));
  const handleRemoveFile = (id: string) => setFiles(files.filter((f) => f.id !== id));
  const handleToggleSelected = (id: string, checked: boolean) => setFiles(files.map((f) => (f.id === id ? { ...f, selected: checked } : f)));
  const handleSelectAllCompleted = (checked: boolean) =>
    setFiles(files.map((f) => (f.status === 'completed' && f.text.trim() ? { ...f, selected: checked } : f)));

  const handleAcceptAll = async () => {
    const phrases = files
      .filter((f) => f.status === 'completed' && f.selected && f.text.trim())
      .map((f) => ({ text_body: f.text.trim(), author: String(f.author || '').trim(), is_published: Number(defaultStatus) }));
    if (!phrases.length) return toast.error('Нет распознанных фраз');
    try {
      const res = await api<{ parsed: number; inserted: number; updated: number }>('/api/phrases/import-text', 'POST', {
        phrases_struct: phrases,
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
    <div className="space-y-6 w-full max-w-full overflow-x-hidden">
      <div className="flex items-center gap-2 text-sm text-zinc-400"><Link to="/phrases" className="hover:text-zinc-200">Фразы</Link><span>/</span><span className="text-zinc-200">Импорт</span></div>

      <div className="flex items-start gap-3 sm:items-center sm:gap-4 min-w-0">
        <Link to="/phrases"><Button variant="ghost" size="sm" className="text-zinc-400 hover:text-zinc-200"><ArrowLeft className="h-4 w-4" /></Button></Link>
        <div className="min-w-0">
          <h1 className="text-3xl font-bold text-zinc-50 break-words">Импорт фраз</h1>
          <p className="text-zinc-400 mt-1 break-words">Добавьте фразы через текст или распознавание изображений</p>
        </div>
      </div>

      <Tabs defaultValue="images" className="w-full">
        <TabsList className="bg-zinc-900 border border-zinc-800 grid grid-cols-1 sm:grid-cols-2 h-auto w-full">
          <TabsTrigger value="images" className="data-[state=active]:bg-zinc-800 data-[state=active]:text-zinc-100 w-full justify-center"><Image className="mr-2 h-4 w-4" />Загрузка изображений</TabsTrigger>
          <TabsTrigger value="text" className="data-[state=active]:bg-zinc-800 data-[state=active]:text-zinc-100 w-full justify-center"><FileText className="mr-2 h-4 w-4" />Вставка текста</TabsTrigger>
        </TabsList>

        <TabsContent value="images" className="space-y-4 mt-6">
          <Card className="bg-zinc-900 border-zinc-800 p-4 sm:p-6 space-y-4 overflow-x-hidden">
            <div
              onDrop={handleDrop}
              onDragOver={(e) => { e.preventDefault(); setIsDragging(true); }}
              onDragLeave={() => setIsDragging(false)}
              className={`border-2 border-dashed rounded-lg p-6 sm:p-12 text-center transition-colors ${isDragging ? 'border-blue-500 bg-blue-950/20' : 'border-zinc-700 hover:border-zinc-600'}`}
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
                <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
                  <p className="text-sm text-zinc-300 break-words">Распознано {processedCount} из {totalCount} • Выбрано {selectedCount}</p>
                  <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 w-full lg:w-auto">
                    <Button size="sm" variant="outline" onClick={() => handleSelectAllCompleted(true)} className="bg-zinc-800 border-zinc-700 text-zinc-300 hover:bg-zinc-700 w-full">Отметить все</Button>
                    <Button size="sm" variant="outline" onClick={() => handleSelectAllCompleted(false)} className="bg-zinc-800 border-zinc-700 text-zinc-300 hover:bg-zinc-700 w-full">Снять все</Button>
                    <Button size="sm" variant="outline" onClick={() => setFiles([])} className="bg-zinc-800 border-zinc-700 text-zinc-300 hover:bg-zinc-700 w-full">Отменить</Button>
                    <Button size="sm" onClick={handleAcceptAll} disabled={selectedCount === 0} className="bg-green-600 hover:bg-green-700 text-white w-full"><Check className="mr-2 h-4 w-4" />Принять все</Button>
                  </div>
                </div>

                <Progress value={totalCount ? (processedCount / totalCount) * 100 : 0} className="h-2" />

                <div className="space-y-2 max-h-96 overflow-y-auto overflow-x-hidden">
                  {files.map((file) => (
                    <div
                      key={file.id}
                      className={`rounded-lg p-4 space-y-2 border ${
                        file.selected ? 'bg-green-950/20 border-green-800' : 'bg-zinc-800 border-zinc-700'
                      }`}
                    >
                      <div className="flex items-start justify-between gap-3 min-w-0">
                        <div className="flex items-start gap-2 sm:gap-3 min-w-0 flex-1">
                          <Checkbox
                            checked={file.selected}
                            disabled={!(file.status === 'completed' && file.text.trim())}
                            onCheckedChange={(checked) => handleToggleSelected(file.id, Boolean(checked))}
                            className="border-zinc-500"
                          />
                          <span className="text-xs text-zinc-400 shrink-0 min-w-[64px]">Сохранить</span>
                          {file.status === 'processing' && <Loader2 className="h-4 w-4 text-blue-400 animate-spin" />}
                          {file.status === 'completed' && <Check className="h-4 w-4 text-green-400" />}
                          {file.status === 'error' && <X className="h-4 w-4 text-red-400" />}
                          <span className="text-sm text-zinc-400 truncate min-w-0">{file.fileName}</span>
                          {file.ocrEngine ? (
                            <span className="text-[11px] px-2 py-0.5 rounded border border-zinc-600 text-zinc-300">
                              OCR: {file.ocrEngine}
                            </span>
                          ) : null}
                        </div>
                        <div className="flex gap-2 shrink-0">
                          {file.status === 'completed' && <Button size="sm" variant="ghost" onClick={() => handleToggleEdit(file.id)} className="text-zinc-400 hover:text-zinc-200 px-2"><Edit2 className="h-4 w-4" /></Button>}
                          <Button size="sm" variant="ghost" onClick={() => handleRemoveFile(file.id)} className="text-red-400 hover:text-red-300 px-2"><X className="h-4 w-4" /></Button>
                        </div>
                      </div>
                      {file.status === 'completed' && (
                        file.editable ? (
                          <div className="space-y-2">
                            <Textarea
                              value={file.text}
                              onChange={(e) => handleEditPhrase(file.id, e.target.value)}
                              rows={4}
                              className="bg-zinc-900 border-zinc-600 text-zinc-100 resize-y min-h-24"
                            />
                            <Input
                              value={file.author || ''}
                              onChange={(e) => handleEditAuthor(file.id, e.target.value)}
                              placeholder="Автор (опционально)"
                              className="bg-zinc-900 border-zinc-600 text-zinc-100"
                            />
                          </div>
                        ) : (
                          <div>
                            <p className="text-zinc-200 whitespace-pre-wrap break-words">{file.text}</p>
                            {file.author ? <p className="text-zinc-400 text-sm mt-1">— {file.author}</p> : null}
                          </div>
                        )
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
                <div className="flex items-center space-x-2"><RadioGroupItem value="0" id="status-new" className="border-zinc-700" /><Label htmlFor="status-new" className="text-zinc-300 cursor-pointer">Новая</Label></div>
                <div className="flex items-center space-x-2"><RadioGroupItem value="1" id="status-published" className="border-zinc-700" /><Label htmlFor="status-published" className="text-zinc-300 cursor-pointer">Опубликованная</Label></div>
              </RadioGroup>
            </div>

            <Button onClick={handleTextImport} disabled={linesCount === 0} className="w-full bg-blue-600 hover:bg-blue-700 text-white">Добавить {linesCount > 0 && `(${linesCount})`}</Button>
          </Card>
        </TabsContent>

        
      </Tabs>
    </div>
  );
}
